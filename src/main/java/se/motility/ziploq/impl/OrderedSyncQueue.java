/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.Queue;

import org.jctools.queues.QueueFactory;
import org.jctools.queues.spec.ConcurrentQueueSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.RuntimeInterruptedException;

/**
 * Queue used for Producers providing an ordered message sequence.
 * 
 * @author M Tegling
 * 
 * @param <E> message type
 * @see SpscSyncQueue
 */
public class OrderedSyncQueue<E> implements SpscSyncQueue<E> {
    
    private static final Logger LOG = LoggerFactory.getLogger(OrderedSyncQueue.class);
    
    private final Queue<Entry<E>> ready;
    private final long nanoWaitTime;
    
    private long lastTs = 0;
    
    OrderedSyncQueue(int capacity) {
        this.ready = QueueFactory.newQueue(ConcurrentQueueSpec.createBoundedSpsc(capacity));
        this.nanoWaitTime = capacity * 50L; //target: 20M events/s
    }

    @Override
    public boolean offer(Entry<E> entry) {
        verifyTimestamp(entry);
        return ready.offer(entry);
    }

    @Override
    public boolean put(Entry<E> entry) {
        verifyTimestamp(entry);
        while (!ready.offer(entry)) {
            if(Thread.currentThread().isInterrupted()) {
                throw new RuntimeInterruptedException("Thread interrupted");
            }
            WaitStrategy.specificWait(nanoWaitTime);
        }
        return true;
    }
    
    @Override
    public void updateSystemTs(long timestamp) {
        //do nothing
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
    public int size() {
        return ready.size();
    }
    
    private void verifyTimestamp(Entry<E> entry) {
        long updTs = entry.getBusinessTs();
        if (updTs < lastTs) {
            LOG.warn("Business timestamp has been updated in non-increasing order. "
                    + "Breaks sorting contract. Last {}, now {}.",
                    lastTs, updTs);
        }
        lastTs = updTs;
    }

}
