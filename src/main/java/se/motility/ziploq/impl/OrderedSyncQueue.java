/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscLinkedQueue;
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
 * @see SyncQueue
 */
public class OrderedSyncQueue<E> implements SyncQueue<E> {
    
    private static final long ONE_MILLISECOND = 1_000_000L; //throughput ~ capacity x 1000 events/s
    private static final Logger LOG = LoggerFactory.getLogger(OrderedSyncQueue.class);
    
    private final MessagePassingQueue<Entry<E>> ready;
    private final int capacity;
    
    private long lastTs = 0;
    
    OrderedSyncQueue(int capacity) {
        this.ready = capacity > 0 ? new MpscArrayQueue<>(capacity) : new MpscLinkedQueue<>();
        this.capacity = ready.capacity(); //retrieve actual capacity (power of 2)
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
            WaitStrategy.specificWait(ONE_MILLISECOND);
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
    
    @Override
    public int readySize() {
        return size();
    }
    
    @Override
    public int remainingCapacity() {
        return capacity - size();
    }
    
    private void verifyTimestamp(Entry<E> entry) {
        if (LOG.isDebugEnabled()) {
            long updTs = entry.getBusinessTs();
            if (updTs < lastTs) {
                LOG.debug("Business timestamp has been updated in non-increasing order. "
                        + "Breaks ordering contract. Last {}, now {}.",
                        lastTs, updTs);
            }
            lastTs = updTs;
        }
    }

}
