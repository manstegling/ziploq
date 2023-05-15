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
import java.util.concurrent.atomic.AtomicLong;

import org.jctools.queues.MpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed job scheduler executing work on a fixed thread pool.
 * <p>
 * Each job is executed locally on a thread until there's either no more work available,
 * or the downstream buffers are full. At that point the job is rescheduled. If the job
 * cannot proceed for {@link #RETRIES} times, it will be put into cooldown.
 * <p>
 * The scheduler maintains a central thread rescheduling jobs in cooldown at a fixed interval,
 * meaning that the upper bound for cooldown is 1ms (plus OS stalls, jitter, etc.).
 * <p>
 * Jobs are being selected fairly, except that jobs in cooldown are not available for selection.
 *
 * @author M Tegling
 *
 * @implNote This is the best candidate: it's simple and provides a good abstraction.
 * It performs on par with the other schedulers, better when tweaked. The tweaking is
 * the only pain point. The worker threads quickly build contention (on the queues)
 * when they have spare bandwidth, leading to lower throughput. We need to more
 * aggressively throttle/sleep threads that are "excessive".
 * TODO: How to detect this??
 * TODO: Doesn't seem to be directly related to RETRIES? Is this the correct diagnosis?
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
    private final ScheduledFuture<?> cooldownTask;

    public JobScheduler(int threads, List<Job> jobs) {
        LOG.debug("Job scheduler config: {} retries", RETRIES);
        int instance = COUNTER.incrementAndGet();
        int effectiveThreads = Math.min(threads, jobs.size());
        this.threads = createWorkers(effectiveThreads, instance);
        this.jobs = jobs.size();
        this.queue = createJobQueue(jobs);
        this.cooldown = new MpmcArrayQueue<>(jobs.size());
        this.cooldownTask = COOLDOWN_EXECUTOR.scheduleWithFixedDelay(this::reset, 1L, 1L, TimeUnit.MILLISECONDS);
    }

    private Thread[] createWorkers(int threadCount, int instanceId) {
        Thread[] workerThreads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Thread worker = new Thread(this::process, "worker-" + instanceId + "-" + i);
            worker.setDaemon(true);
            workerThreads[i] = worker;
        }
        return workerThreads;
    }

    private Queue<JobContext> createJobQueue(List<Job> jobs) {
        Queue<JobContext> jobQueue = new MpmcArrayQueue<>(jobs.size());
        jobs.forEach(job -> sanityCheck(jobQueue.offer(new JobContext(job))));
        return jobQueue;
    }

    public void start() {
        for (Thread t : threads) {
            t.start();
        }
    }

    private void process() {
        JobContext context;
        int emptyCycles;
        while (!Thread.currentThread().isInterrupted()) {
            emptyCycles = 0;
            while ((context = queue.poll()) == null) {
                long wait = Math.min((int)Math.pow(10, emptyCycles++), 1_000_000L); //TODO: improve formulation
                LOG.debug("Sleeping {}...", wait);
                WaitStrategy.specificWait(wait);
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

            // Handle batch end state accordingly
            if (state == Job.State.WAITING || state == Job.State.BLOCKED) {
                int attempt = context.attempts++;
                context.delayStats[attempt < RETRIES ? 0 : 1]++;
                context.cycles++;
                if (attempt > RETRIES) {
                    sanityCheck(cooldown.offer(context));
                } else {
                    sanityCheck(queue.offer(context));
                }
            } else if (state == Job.State.COMPLETED) {
                LOG.info("Job completed. Total cycles: {}, Stats: {}, {}",
                        context.cycles, context.delayStats, context.processedStats);
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
            sanityCheck(queue.offer(ctx));
        }
    }

    private static class JobContext {
        private final int[] delayStats = new int[2];
        private final int[] processedStats = new int[8];
        private final Job job;
        private int attempts = 0;
        private long cycles = 0L;

        public JobContext(Job job) {
            this.job = job;
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
