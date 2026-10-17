/*
 * Copyright (c) 2022-2023 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.jctools.queues.MpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed job scheduler executing work on a fixed thread pool.
 * <p>
 * Each job is executed locally on a thread until there's either no more work available
 * {@link Job.State#WAITING}, or the downstream buffers are full {@link Job.State#BLOCKED}.
 * At that point the job is being rescheduled.
 * <p>
 * Upon WAITING, a number of retries are made to immediately catch any new work. After
 * {@link #RETRIES} times, the Job is put into cooldown. The scheduler maintains a central
 * thread rescheduling jobs in cooldown at a fixed interval, meaning that the upper bound
 * for cooldown is 1ms (plus OS stalls, jitter, etc.).
 * <p>
 * Upon BLOCKING, the Job is parked until a significant number of events has been successfully
 * drained from the associated work queue. Then, the Job is resumed since there's now sufficient
 * capacity to meaningfully fill again. This is an efficient event-driven back-pressure mechanism.
 * <p>
 * Jobs are being selected fairly, except that jobs in cooldown are not available for selection.
 * Worker threads are put to (short) sleep if all buffers are full, to minimize thrashing and
 * wasting CPU.
 *
 * @author M Tegling
 *
 */
public class JobScheduler {

    /** Global executor for scheduling jobs in cooldown */
    private static final ScheduledExecutorService COOLDOWN_EXECUTOR =
            Executors.newScheduledThreadPool(1, new CooldownThreadFactory());

    private static final int RETRIES = Integer.getInteger("ziploq.dev.retries", 20);
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

    private final Thread[] threads;
    private final Queue<JobContext> queue;
    private final Queue<JobContext> cooldown;
    private final int jobs;
    private final AtomicInteger completed = new AtomicInteger();
    private final AtomicLong totalCycles = new AtomicLong();
    private final AtomicIntegerArray parked;
    private final ScheduledFuture<?> cooldownTask;

    public JobScheduler(int threads, List<Job> jobs) {
        LOG.debug("Job scheduler config: {} retries", RETRIES);
        int instance = COUNTER.incrementAndGet();
        int effectiveThreads = Math.min(threads, jobs.size());
        this.parked = new AtomicIntegerArray(effectiveThreads);
        this.threads = createWorkers(effectiveThreads, instance);
        this.jobs = jobs.size();
        this.queue = createJobQueue(jobs, this::enqueue);
        this.cooldown = new MpmcArrayQueue<>(jobs.size());
        this.cooldownTask = COOLDOWN_EXECUTOR.scheduleWithFixedDelay(this::reset, 1L, 1L, TimeUnit.MILLISECONDS);
    }

    private Thread[] createWorkers(int threadCount, int instanceId) {
        Thread[] workerThreads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            Thread worker = new Thread(() -> process(threadId), "worker-" + instanceId + "-" + i);
            worker.setDaemon(true);
            workerThreads[i] = worker;
        }
        return workerThreads;
    }

    private static Queue<JobContext> createJobQueue(List<Job> jobs, Consumer<JobContext> enqueue) {
        Queue<JobContext> jobQueue = new MpmcArrayQueue<>(jobs.size());
        jobs.forEach(job -> sanityCheck(jobQueue.offer(new JobContext(job, enqueue))));
        return jobQueue;
    }

    public void start() {
        for (Thread t : threads) {
            t.start();
        }
    }

    private void process(int threadId) {
        JobContext context;
        while (!Thread.currentThread().isInterrupted()) {
            context = nextJob(threadId);
            if (context == null) {
                return; // shutdown
            }

            // Dynamic batching, invoke until not possible anymore
            Job.State state = doWork(context);
            int processed = 0;
            while (state == Job.State.READY) {
                context.attempts = 0;
                state = doWork(context);
                processed++;
            }
            context.processedStats[getIdx(processed)]++;
            context.cycles++;

            // Handle batch end state accordingly
            if (state == Job.State.BLOCKED) {
                context.job.requestNotifyOnDrain(context.resume);
            } else if (state == Job.State.WAITING) {
                int attempt = context.attempts++;
                context.delayStats[attempt < RETRIES ? 0 : 1]++;
                if (attempt > RETRIES) {
                    sanityCheck(cooldown.offer(context));
                } else {
                    enqueue(context);
                }
            } else if (state == Job.State.COMPLETED) {
                LOG.info("Job completed for source '{}' (ID: '{}'). Total cycles: {}, Stats: {}, {}",
                        context.job.getSourceName(), context.job.getConsumerId(), context.cycles,
                        context.delayStats, context.processedStats);
                totalCycles.addAndGet(context.cycles);
                int totalCompleted = completed.incrementAndGet();
                if (totalCompleted > jobs - threads.length) {
                    // this thread is excessive, pin remaining threads?
                    if (totalCompleted == jobs) {
                        cooldownTask.cancel(false);
                        LOG.info("Total cycles in scheduler: {}", totalCycles.get());
                    }
                    return;
                }
            } else {
                throw new IllegalStateException("Unknown status: " + state);
            }
        }
    }

    private JobContext nextJob(int threadId) {
        JobContext context;
        for (;;) {
            if ((context = queue.poll()) != null) {
                return context;
            }
            parked.set(threadId, 1);

            // Defend against race with parked job drain notifier
            if ((context = queue.poll()) != null) {
                parked.compareAndSet(threadId, 1, 0);
                return context;
            }

            // Loop to handle spurious wakeup
            while (parked.get(threadId) != 0) {
                if (Thread.currentThread().isInterrupted()) {
                    parked.compareAndSet(threadId, 1, 0);
                    return null; // signal shutdown
                }
                LockSupport.park(this);
            }
        }
    }

    private void enqueue(JobContext context) {
        sanityCheck(queue.offer(context));
        // Wake one (or none if none is parked)
        for (int i = 0; i < threads.length; i++) {
            if (parked.get(i) != 0 && parked.compareAndSet(i, 1, 0)) {
                LockSupport.unpark(threads[i]);
                return;
            }
        }
    }

    private static void sanityCheck(boolean accepted) {
        if (!accepted) {
            throw new IllegalStateException("Job has unexpectedly been dropped");
        }
    }

    private static int getIdx(int processed) {
        if (processed < 1) {
            return 0;
        } else if (processed < 2) {
            return 1;
        } else if (processed < 11) {
            return 2;
        } else if (processed < 101) {
            return 3;
        } else if (processed < 1001) {
            return 4;
        } else if (processed < 10001) {
            return 5;
        } else if (processed < 30001) {
            return 6;
        } else {
            return 7;
        }
    }

    private Job.State doWork(JobContext ctx) {
        try {
            return ctx.job.invoke();
        } catch (RuntimeException e) {
            LOG.error("Exception:", e);
            //TODO
            return Job.State.READY;
        }
    }

    private void reset() {
        JobContext ctx;
        while ((ctx = cooldown.poll()) != null) {
            enqueue(ctx);
        }
    }

    private static class JobContext {
        private final int[] delayStats = new int[2];
        private final int[] processedStats = new int[8];
        private final Job job;
        private final Runnable resume;
        private int attempts = 0;
        private long cycles = 0L;

        public JobContext(Job job, Consumer<JobContext> enqueue) {
            this.job = job;
            this.resume = () -> enqueue.accept(this);
        }
    }

    public static class CooldownThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "JobScheduler-cooldown-" + threadNumber.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }

}
