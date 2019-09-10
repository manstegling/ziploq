/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.concurrent.atomic.AtomicInteger;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.SynchronizedConsumer;

/**
 * Implementation of {@link SynchronizedConsumer} for use with {@link ZiploqImpl}.
 * <p>
 * Everything that is not part of the {@code SynchronizedConsumer} API is set to
 * <i>package-private</i>, since it should only be accessed by {@code ZiploqImpl}.
 * 
 * @author M Tegling
 *
 * @param <T> message type
 */
public class SynchronizedConsumerImpl<T> implements SynchronizedConsumer<T> {
    
    private static final AtomicInteger ID_GEN = new AtomicInteger(0);
    private static final String ID_PREFIX = "SyncQueue-";
    private static final long END = Long.MAX_VALUE;
    
    private final SpscSyncQueue<T> queue;
    private final long systemDelay;
    private final String id;
    private final BackPressureStrategy strategy;
    private final Runnable signalUpdate;
    
    private volatile long system = 0L; //start from 0 to prevent underflow
    private volatile boolean isComplete = false;
    private volatile boolean hasChanged = false;
    private volatile long graceExpiry = 0L;
    private volatile long lastSystem = 0L;
    
    private boolean inHeads = false; //only ever modified by Ziploq output thread
    
    SynchronizedConsumerImpl(SpscSyncQueue<T> queue, long systemDelay,
            BackPressureStrategy strategy, Runnable signalUpdate, String name) {
        this.queue = queue;
        this.systemDelay = systemDelay;
        this.id = ID_PREFIX + ID_GEN.incrementAndGet() + "-" + name;
        this.strategy = strategy;
        this.signalUpdate = signalUpdate;
    }
    
    long getSystemTs() {
        return system;
    }

    boolean isInHeads() {
        return inHeads;
    }

    void setInHeads(boolean inHeads) {
        this.inHeads = inHeads;
    }
    
    boolean verifyCheckpoint() {
        return !hasChanged;
    }
    
    void setCheckpoint() {
        this.hasChanged = false;
    }
    
    boolean isComplete() {
        return isComplete;
    }
    
    @SuppressWarnings("unchecked")
    <E> EntryImpl<E> poll() {
        //safe cast since EntryImpl is immutable
        return (EntryImpl<E>) queue.poll();
    }

    @Override
    public boolean onEvent(T item, long businessTs, long systemTs) {
        if (isComplete) {
            throw new IllegalStateException(
                    "Consumer has already completed. New events are not allowed.");
        }
        boolean accepted = strategy == BackPressureStrategy.BLOCK
            ? queue.put(new EntryImpl<>(item, businessTs, systemTs, this))
            : queue.offer(new EntryImpl<>(item, businessTs, systemTs, this));
        if (systemTs - lastSystem > systemDelay) {
            //The producer has made a sudden jump in system time. Wait for recovery.
            graceExpiry = systemTs + systemDelay;
        }
        onEvent(systemTs);
        return accepted;
    }
    
    @Override
    public void updateSystemTime(long timestamp) {
        if (isComplete) {
            throw new IllegalStateException(
                    "Consumer has already completed. Updating system time is not allowed.");
        }
        queue.updateSystemTs(timestamp);
        if (graceExpiry > timestamp) {
            //Calling updateSystemTime means we've recovered
            graceExpiry = timestamp;
        }
        onEvent(timestamp);
    }
    
    @Override
    public void complete() {
        queue.updateSystemTs(END);
        isComplete = true;
        onEvent(END);
    }

    @Override
    public BackPressureStrategy getStrategy() {
        return strategy;
    }
    
    @Override
    public String getId() {
        return id;
    }

    void onEvent(long ts) {
        if (ts > system && ts >= graceExpiry) {
            hasChanged = true; //must come before write to 'system'
            system = ts;
            signalUpdate.run();
        }
        lastSystem = ts;
    }
}
