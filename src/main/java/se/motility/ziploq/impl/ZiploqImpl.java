/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.impl.SpscSyncQueueFactory.CapacityType;

/**
 * Lock-free implementation of {@link Ziploq} and {@link ZipFlow}.
 * <p>
 * The Producer threads are responsible for the work of managing their respective buffers.
 * In case of unordered input data, the associated Producer thread is hence also responsible
 * for the sequencing work for the input data (intra-channel sequencing).
 * <p>
 * The Consumer thread is responsible for sequencing messages <i>between</i> input sources
 * (inter-channel sequencing).
 * <p>
 * This separation of concern restricts memory synchronization to two points:
 * <ol>
 *   <li>Enqueue/Dequeue operations in source-specific message buffers</li>
 *   <li>Testing if message should be emitted based on system timestamp</li>
 * </ol>
 * The first one is unfortunately unavoidable when moving messages from one thread to another,
 * but the impact is limited as much as possible by using lock-free concurrent queues.
 * <p>
 * The second one is only needed when one or more of the input sources are silent while system
 * time is progressing. Efficient use of memory barriers limits the cost of synchronization.
 * If the {@code ZipFlow} functionality is not needed, setting {@code systemDelay} to 0 and
 * using the {@code Ziploq} facade will provide a simpler API and grant some additional performance.
 * <p>
 * It should be noted that there's a third synchronization point; when additional
 * input sources are registered. However, this is implemented such that it does not affect
 * message-passing performance.
 * 
 * @author M Tegling
 *
 * @param <E> message type
 * @see ZipFlow
 * @see Ziploq
 */
public class ZiploqImpl<E> implements ZipFlow<E> {

    private static final boolean COMPARATOR_COMPLIANT = Boolean.getBoolean("ziploq.log.comparator_compliant");
    private static final long WAIT_TIMEOUT = Long.getLong("ziploq.log.wait_timeout", 120_000L);
    private static final Logger LOG = LoggerFactory.getLogger(ZiploqImpl.class);
    
    @SuppressWarnings("rawtypes")
    private static final EntryImpl OUT_OF_SYNC = new EntryImpl<>(null, -1, -1, null);

    private final List<FlowConsumerImpl<? extends E>> queues = new ArrayList<>();
    private final Queue<FlowConsumerImpl<? extends E>> updQueues = new ConcurrentLinkedQueue<>();    
    
    private final PriorityQueue<EntryImpl<E>> heads;
    private final long systemDelay;
    private final Comparator<Entry<E>> effectiveComparator;
    private final Comparator<E> secondaryComparator;
    
    private volatile boolean dirtyQueues   = true;
    private volatile boolean dirtySystemTs = true;
    private volatile boolean complete      = false;
    private long systemTs = 0L;  //start from 0 to prevent underflow
    private EntryImpl<E> previous;
    private long lastLoggedWait;
    
    public ZiploqImpl(long systemDelay, Comparator<E> comparator) {
        Comparator<Entry<E>> primaryCmp = Comparator.comparingLong(Entry::getBusinessTs);
        this.effectiveComparator = comparator == null ? primaryCmp :
            primaryCmp.thenComparing(Entry::getMessage, comparator);
        this.heads = new PriorityQueue<>(effectiveComparator);
        this.systemDelay = systemDelay;
        this.secondaryComparator = comparator;
    }
    
    @Override
    public Entry<E> take() throws InterruptedException {
        int attempt = 1;
        Entry<E> entry;
        while((entry = dequeue()) == null) {
            if(complete) {
                return Ziploq.getEndSignal();
            } else if (Thread.interrupted()) {
                throw new InterruptedException("Thread interrupted.");
            }
            checkWait(attempt);
            WaitStrategy.backOffWait(attempt++);
        }
        return entry;
    }
    
    @Override
    public Entry<E> poll() {
        Entry<E> entry = dequeue();
        return entry == null && complete ?
                Ziploq.getEndSignal() : entry;
    }
    
    @Override
    public <T extends E> FlowConsumer<T> registerUnordered(
            long businessDelay, int softCapacity, BackPressureStrategy strategy,
            String sourceName, Comparator<T> comparator) {
        Comparator<T> effectiveCmp;
        if (secondaryComparator != null) {
            @SuppressWarnings("unchecked") //downcasting type parameter for Comparator is safe
            Comparator<T> cmp = (Comparator<T>) secondaryComparator;
            effectiveCmp = comparator != null ?
                    cmp.thenComparing(comparator) : cmp;
        } else {
            effectiveCmp = comparator;
        }
        CapacityType type = strategy == BackPressureStrategy.UNBOUNDED ?
                CapacityType.UNBOUNDED : CapacityType.BOUNDED;
        SpscSyncQueue<T> queue = SpscSyncQueueFactory.createUnordered(
                businessDelay, systemDelay, softCapacity, type, effectiveCmp);
        return register(queue, false, strategy, sourceName);
    }
    
    @Override
    public <T extends E> FlowConsumer<T> registerOrdered(int capacity,
            BackPressureStrategy strategy, String sourceName) {
        CapacityType type = strategy == BackPressureStrategy.UNBOUNDED ?
                CapacityType.UNBOUNDED : CapacityType.BOUNDED;
        SpscSyncQueue<T> queue = SpscSyncQueueFactory.createOrdered(capacity, type);
        return register(queue, true, strategy, sourceName);
    }
    
    @Override
    public Comparator<Entry<E>> getComparator() {
        return effectiveComparator;
    }
    
    private <T extends E> FlowConsumer<T> register(SpscSyncQueue<T> queue,
            boolean ordered, BackPressureStrategy strategy, String name) {
        ArgChecker.notNull(strategy, "backPressureStrategy");
        FlowConsumerImpl<T> q = new FlowConsumerImpl<>(
                queue, systemDelay, strategy, this::signalDirtySystemTs, name);
        LOG.info("Registering {} input source with name '{}' (ID: {})",
                ordered ? "ordered" : "unordered", name, q.getId());
        updQueues.add(q);
        complete = false; //possibility to re-use Ziploq
        dirtyQueues = true;
        return q;
    }
    
    private void signalDirtySystemTs() {
        this.dirtySystemTs = true;
    }
    
    private Entry<E> dequeue() {
        while (true) {
            updateQueues();
            updateHeads();
            EntryImpl<E> ready = pollReadyMsg();
            if(ready == OUT_OF_SYNC) {
                //perform one more cycle
            } else if (ready != null) {
                ready.getQueueRef().setInHeads(false);
                checkMessageOrder(ready);
                return ready;
            } else {
                return null;
            }
        }
    }
    
    private void updateQueues() {
        if (dirtyQueues) {
            dirtyQueues = false; //must come first
            FlowConsumerImpl<? extends E> q;
            while((q = updQueues.poll()) != null) {
                queues.add(q);
            }
            dirtySystemTs = true;
        }
    }

    private void updateHeads() {
        int deregister = 0;
        for (int i = 0; i < queues.size(); i++) {
            FlowConsumerImpl<? extends E> queue = queues.get(i);
            queue.setCheckpoint();
            if (!queue.isInHeads()) {
                EntryImpl<E> polled = queue.poll();
                if (polled != null) {
                    heads.offer(polled); //always true
                    queue.setInHeads(true);
                } else if (queue.isComplete()) {
                    deregister++;
                }
            }
        }
        if (deregister > 0) {
            queues.removeIf(q -> {
                if (!q.isInHeads() && q.isComplete()) {
                    LOG.info("De-registering completed consumer with ID '{}'.",
                            q.getId());
                    return true;
                } else {
                    return false;
                }
            });
            if (queues.isEmpty()) {
                LOG.info("All consumers de-registered.");
                complete = true;
            }
        }
    }
    
    private EntryImpl<E> pollReadyMsg() {
        if (heads.size() == queues.size()) {
            return heads.poll();
        }
        return systemDelay > 0 ? pollSystemReadyMsg() : null;
    }
    
    private EntryImpl<E> pollSystemReadyMsg() {
        EntryImpl<E> peeked = heads.peek();
        if (peeked != null) {
            if (dirtySystemTs) {
                if (!updateLatestSystemTs()) {
                    return getOutOfSyncMarker();
                }
                dirtySystemTs = false;
            }
            if (systemTs - peeked.getSystemTs() > systemDelay) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Message with business timestamp {} dequeued based on system timestamp progress. "
                            + "Global system timestamp: {}, Message system timestamp: {}",
                            peeked.getBusinessTs(), systemTs, peeked.getSystemTs());
                }
                return heads.poll();
            }
        }
        return null;
    }
    
    //'false' means out-of-sync; need to restart dequeue cycle
    boolean updateLatestSystemTs() {
        long ts2Update = Long.MAX_VALUE;
        for (int i = 0; i < queues.size(); i++) {
            FlowConsumerImpl<? extends E> q = queues.get(i);
            if (q.getSystemTs() < ts2Update) {
                ts2Update = q.getSystemTs();
            }
            if(!q.verifyCheckpoint()) {
                return false;
            }
        }
        systemTs = ts2Update;
        return true;
    }
    
    private void checkWait(int attempt) {
        if (attempt == 1) {
            lastLoggedWait = System.currentTimeMillis();
        } else if (attempt % 1_000 == 0) {
            long now = System.currentTimeMillis();
            if (now - lastLoggedWait > WAIT_TIMEOUT) {
                lastLoggedWait = now;
                List<String> emptySources = queues.stream()
                        .filter(q -> !q.isInHeads())
                        .map(FlowConsumerImpl::getId)
                        .sorted()
                        .collect(Collectors.toList());
                LOG.warn("Not receiving messages for input source(s): {}", emptySources);
            }
        }
    }
    
    private void checkMessageOrder(EntryImpl<E> next) {
        if (previous != null) {
            boolean outOfSequence = COMPARATOR_COMPLIANT
                    ? effectiveComparator.compare(previous, next) > 0 
                    : previous.getBusinessTs() > next.getBusinessTs();
            if (outOfSequence) {
                LOG.warn("Entry dispatched out-of-sequence. The source of one of the following messages has "
                        + "violated the contract. Previous message: {}, New message: {}", previous, next);
            }
        }
        previous = next;
    }
    
    @SuppressWarnings("unchecked")
    private EntryImpl<E> getOutOfSyncMarker() {
        return OUT_OF_SYNC;
    }
    
}
