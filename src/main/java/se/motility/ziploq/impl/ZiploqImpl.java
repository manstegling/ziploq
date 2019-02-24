/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.RuntimeInterruptedException;
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;

/**
 * Lock-free implementation of {@link Ziploq}.
 * <p>
 * The Producer threads are responsible for the work of managing their respective buffers.
 * In case of unordered input data, the associated Producer thread is hence also responsible
 * for the sequencing work for the input data (intra-channel sequencing).
 * <p>
 * The Consumer thread is responsible for sequencing messages <i>between</i> input sources
 * (inter-channel sequencing).
 * <p>
 * This separation of concern restricts memory synchronization to two points:
 * <ul>
 * <li>Enqueue/Dequeue operations in source-specific message buffers</li>
 * <li>Testing if message should be emitted based on system timestamp</li>
 * </ul>
 * The first one is unfortunately unavoidable when moving messages from one thread to another,
 * but the impact is limited as much as possible by using lock-free concurrent queues.
 * <p>
 * The second one is only needed when one or more of the input sources are silent while system
 * time is progressing. Efficient use of memory barriers limits the cost of synchronization.
 * <p>
 * It should be noted that there's a third synchronization point; when additional
 * input sources are registered. However, this is implemented such that it does not affect
 * message-passing performance.
 * 
 * @author M Tegling
 *
 * @param <E> message type
 * @see Ziploq
 */
public class ZiploqImpl<E> implements Ziploq<E> {

    private static final Logger LOG = LoggerFactory.getLogger(ZiploqImpl.class);

    @SuppressWarnings("rawtypes")
    private static final EntryImpl OUT_OF_SYNC = new EntryImpl<>(null, -1, -1, null);
    
    private final Comparator<Entry<E>> primaryComparator = 
            Comparator.comparingLong(Entry::getBusinessTs);

    private final List<SynchronizedConsumerImpl<? extends E>> queues = new ArrayList<>();
    private final Queue<SynchronizedConsumerImpl<? extends E>> updQueues = new ConcurrentLinkedQueue<>();    
    
    private final PriorityQueue<EntryImpl<E>> heads;
    private final long systemDelay;
    private final Optional<Comparator<E>> secondaryComparator;
    
    private volatile boolean dirtyQueues = true;
    private volatile boolean dirtySystemTs = true;
    private volatile boolean complete = false;
    private long systemTs = 0L;         //start from 0 to prevent underflow
    private long lastTs;
    
    public ZiploqImpl(long systemDelay, Optional<Comparator<E>> comparator) {
        Comparator<Entry<E>> effectiveCmp = !comparator.isPresent() ? primaryComparator :
                primaryComparator.thenComparing(Entry::getMessage, comparator.get());
        this.heads = new PriorityQueue<>(effectiveCmp);
        this.systemDelay = systemDelay;
        this.secondaryComparator = comparator;
    }
    
    @Override
    public Stream<Entry<E>> stream() {
        return StreamSupport.stream(new Splitr(), false);
    }
    
    @Override
    public Entry<E> take() throws InterruptedException {
        int attempt = 0;
        Entry<E> entry;
        while((entry = dequeue()) == null) {
            if(complete) {
                return Ziploq.getEndSignal();
            } else if (Thread.interrupted()) {
                throw new InterruptedException("Thread interrupted.");
            }
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
    public <T extends E> SynchronizedConsumer<T> registerUnordered(
            long businessDelay, int softCapacity, BackPressureStrategy strategy,
            Optional<Comparator<T>> comparator) {
        ArgChecker.notNull(comparator, "comparator (Optional!)");
        Optional<Comparator<T>> effectiveCmp;
        if (secondaryComparator.isPresent()) {
            @SuppressWarnings("unchecked") //downcasting type parameter for Comparator is safe
            Comparator<T> cmp = (Comparator<T>) secondaryComparator.get();
            effectiveCmp = Optional.of(!comparator.isPresent() ? cmp :
                cmp.thenComparing(comparator.get()));
        } else {
            effectiveCmp = comparator;
        }
        SpscSyncQueue<T> queue = SpscSyncQueueFactory.createUnordered(
                businessDelay, systemDelay, softCapacity, effectiveCmp);
        return register(queue, false, strategy);
    }
    
    @Override
    public <T extends E> SynchronizedConsumer<T> registerOrdered(int capacity,
            BackPressureStrategy strategy) {
        SpscSyncQueue<T> queue = SpscSyncQueueFactory.createOrdered(capacity);
        return register(queue, true, strategy);
    }
    
    private <T extends E> SynchronizedConsumer<T> register(SpscSyncQueue<T> queue,
            boolean ordered, BackPressureStrategy strategy) {
        ArgChecker.notNull(strategy, "backPressureStrategy");
        SynchronizedConsumerImpl<T> q = new SynchronizedConsumerImpl<>(queue, strategy, this::signalDirtySystemTs);
        LOG.info("Registering {} input source with ID '{}'",
                ordered ? "ordered" : "unordered", q.getId());
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
                if(LOG.isDebugEnabled()) {
                    debugMessageOrder(ready);
                }
                return ready;
            } else {
                return null;
            }
        }
    }
    
    private void debugMessageOrder(EntryImpl<E> next) {
        long updTs = next.getBusinessTs();
        if(updTs < lastTs) {
            LOG.debug("Entry dispatched out-of-sequence."
                    + "Last timestamp: {}, new timestamp: {}, message: {}", 
                    lastTs, updTs, next.getMessage());
        } else {
            lastTs = updTs;
        }
    }
    
    private void updateQueues() {
        if(dirtyQueues) {
            dirtyQueues = false; //must come first
            SynchronizedConsumerImpl<? extends E> q;
            while((q = updQueues.poll()) != null) {
                queues.add(q);
            }
            dirtySystemTs = true;
        }
    }

    private void updateHeads() {
        int deregister = 0;
        for (int i = 0; i < queues.size(); i++) {
            SynchronizedConsumerImpl<? extends E> queue = queues.get(i);
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
        EntryImpl<E> peeked = heads.peek();
        if (peeked != null) {
            if (dirtySystemTs) {
                if (!updateLatestSystemTs()) {
                    return getOutOfSyncMarker();
                }
                dirtySystemTs = false;
            }
            if (systemTs - peeked.getSystemTs() > systemDelay) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Message dequeued based on system timestamp progress. "
                            + "Global system timestamp: {}, Message system timestamp: {}",
                            systemTs, peeked.getSystemTs());
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
            SynchronizedConsumerImpl<? extends E> q = queues.get(i);
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
    
    @SuppressWarnings("unchecked")
    private EntryImpl<E> getOutOfSyncMarker() {
        return OUT_OF_SYNC;
    }
    
    private class Splitr implements Spliterator<Entry<E>> {

        @Override
        public boolean tryAdvance(Consumer<? super Entry<E>> action) {
            Entry<E> entry;
            try {
                entry = take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeInterruptedException("Thread interrupted.", e);
            }
            if(entry != Ziploq.getEndSignal()) {
                action.accept(entry);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Spliterator<Entry<E>> trySplit() {
            return null; //never split
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.SORTED | Spliterator.NONNULL;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Comparator<Entry<E>> getComparator() {
            return (Comparator<Entry<E>>) heads.comparator();
        }
        
    }
    
}
