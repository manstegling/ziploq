/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;

import org.jctools.queues.QueueFactory;
import org.jctools.queues.spec.ConcurrentQueueSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.RuntimeInterruptedException;

/**
 * Queue used for Producers providing an unordered message sequence.
 * <p>
 * Messages are sequenced first according to <i>business timestamp</i>, then by associated
 * {@code Ziploq}'s {@code Comparator} and last by configured {@code Comparator}.
 * 
 * @author M Tegling
 *
 * @param <E> message type
 * @see SpscSyncQueue
 */
public class UnorderedSyncQueue<E> implements SpscSyncQueue<E> {
    
    private static final long NANO_WAIT = 1_000_000L; //1ms: throughput ~ capacity x 1000 events/s
    private final Comparator<Entry<E>> comparator = Comparator.comparingLong(Entry::getBusinessTs);
    
    private static final Logger LOG = LoggerFactory.getLogger(UnorderedSyncQueue.class);
    
    private final Queue<Entry<E>> staging;
    private final Queue<Entry<E>> ready;
    private final long businessDelay;
    private final long systemDelay;
    private final int softCapacity;
    
    private long ts1Max = 0L; //start from 0 to prevent underflow
    private long ts2Max = 0L;
    private int lSize; //store Producer thread's local size guess to avoid unnecessary size() traversals
    
    UnorderedSyncQueue(long businessDelay, long systemDelay, int softCapacity, Optional<Comparator<E>> comparator) {
        Comparator<Entry<E>> cmp = comparator
                .map(c -> this.comparator.thenComparing(Entry::getMessage, c))
                .orElse(this.comparator);
        this.staging = new PriorityQueue<>(cmp);
        this.ready = QueueFactory.newQueue(ConcurrentQueueSpec.createBoundedSpsc(0)); //unbounded
        this.businessDelay = businessDelay;
        this.systemDelay = systemDelay;
        this.softCapacity = softCapacity;
    }
    
    @Override
    public Entry<E> poll() {
        return ready.poll();
    }
    
    @Override
    public Entry<E> peek() {
        return ready.peek();
    }
    
    @Override
    public boolean offer(Entry<E> entry) {
        verifyTimestamps(entry.getBusinessTs(), entry.getSystemTs());
        return enqueue(entry);
    }
    
    @Override
    public boolean put(Entry<E> entry) {
        verifyTimestamps(entry.getBusinessTs(), entry.getSystemTs());
        while (!enqueue(entry)) {
            if(Thread.currentThread().isInterrupted()) {
                throw new RuntimeInterruptedException("Thread interrupted");
            }
            WaitStrategy.specificWait(NANO_WAIT);
        }
        return true;
    }
    
    private void verifyTimestamps(long businessTs, long systemTs) {
        if (LOG.isDebugEnabled()) {
            if (ts1Max - businessTs > businessDelay) {
                LOG.debug("Item arrived too late (business timestamp). "
                        + "Breaks ordering contract. Max {}, now {}.",
                        ts1Max, businessTs);
            }
            if (systemTs < ts2Max) {
                LOG.debug("System timestamp has been updated in non-increasing order. "
                        + "Breaks ordering contract. Max {}, now {}.",
                        ts2Max, systemTs);
            }
        }
    }

    @Override
    public void updateSystemTs(long systemTs) {
        verifyTimestamps(Long.MAX_VALUE, systemTs);
        if (systemTs > ts2Max) {
            ts2Max = systemTs;
        }
        promoteMessages();
    }
    
    @Override
    public int size() {
        //Total size; including messages not ready yet
        return staging.size() + readySize();
    }
    
    @Override
    public int readySize() {
        return ready.size();
    }
    
    @Override
    public int remainingCapacity() {
        return Math.max(softCapacity - readySize(), 0);
    }
    
    private boolean enqueue(Entry<E> entry) {
        updateVectorClock(entry);
        promoteMessages();
        if (lSize >= softCapacity) {
            lSize = readySize(); //update guess to actual size ("costly")
        }
        return lSize < softCapacity && staging.offer(entry);
    }
    
    private void updateVectorClock(Entry<E> entry) {
        long businessTs = entry.getBusinessTs();
        if (businessTs > ts1Max) {
            ts1Max = businessTs;
        }
        long systemTs = entry.getSystemTs();
        if (systemTs > ts2Max) {
            ts2Max = systemTs;
        }
    }
    
    private boolean isInputReady() {
        Entry<E> tmpPeek = staging.peek();
        return tmpPeek != null && (
                ts1Max - tmpPeek.getBusinessTs() >= businessDelay ||
                ts2Max - tmpPeek.getSystemTs() > systemDelay);
    }
    
    private void promoteMessages() {
        while (isInputReady()) {
            ready.add(staging.poll());
            lSize++;
        }
    }

}
