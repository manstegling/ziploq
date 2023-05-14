/*
 * Copyright (c) 2018-2022 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.concurrent.atomic.AtomicInteger;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;

/**
 * Implementation of {@link FlowConsumer}.
 * <p>
 * Everything that is not part of the {@code FlowConsumer} API is set to
 * <i>package-private</i>, since it should only be accessed by {@code ZiploqImpl}.
 * <p>
 * This consumer handles disconnects by padding any sudden jumps in system time (from an
 * event perspective) by a recovery time. This guarantees no messages are incorrectly 
 * pushed prematurely after reconnecting to the input source.
 * 
 * @author M Tegling
 *
 * @param <T> message type
 */
public class FlowConsumerImpl<T> implements FlowConsumer<T> {
    
    protected static final long END = Long.MAX_VALUE;
    
    private static final AtomicInteger ID_GEN = new AtomicInteger(0);
    private static final String ID_PREFIX = "SyncQueue-";
    
    private final SyncQueue<T> queue;
    private final String id;
    private final BackPressureStrategy strategy;
    private final long systemDelay;
    private final Runnable signalUpdate;
    
    private volatile boolean isComplete = false;
    private volatile boolean hasChanged = false;
    private volatile long system        = 0L;
    private volatile long graceExpiry   = 0L;
    private volatile long lastSystem    = 0L; //start from 0 to prevent underflow
    
    FlowConsumerImpl(SyncQueue<T> queue, long systemDelay,
            BackPressureStrategy strategy, Runnable signalUpdate, String name) {
        this.queue = queue;
        this.id = ID_PREFIX + ID_GEN.incrementAndGet() + "-" + name;
        this.strategy = strategy;
        this.systemDelay = systemDelay;
        this.signalUpdate = signalUpdate;
    }
    
    long getSystemTs() {
        return system;
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
    
    @Override
    public boolean onEvent(T message, long businessTs) {
        return onEvent(message, businessTs, lastSystem);
    }
    
    @Override
    public boolean onEvent(T message, long businessTs, long systemTs) {
        if (isComplete()) {
            throw new IllegalStateException(
                    "Consumer has already completed. New events are not allowed.");
        }
        boolean accepted = strategy == BackPressureStrategy.BLOCK
                ? queue.put(new EntryImpl<>(message, businessTs, systemTs, this))
                : queue.offer(new EntryImpl<>(message, businessTs, systemTs, this));
        if (systemTs - lastSystem > systemDelay) {
            //The producer has made a sudden jump in system time. Wait for recovery.
            graceExpiry = systemTs + systemDelay;
        }
        onEvent(systemTs);
        return accepted;
    }

    @Override
    public void updateSystemTime(long systemTs) {
        if (isComplete()) {
            throw new IllegalStateException(
                    "Consumer has already completed. Updating system time is not allowed.");
        }
        queue.updateSystemTs(systemTs);
        if (graceExpiry > systemTs) {
            //Calling updateSystemTime means we've recovered
            graceExpiry = systemTs;
        }
        onEvent(systemTs);
    }
    
    @SuppressWarnings("unchecked")
    <E> EntryImpl<E> poll() {
        //safe cast since EntryImpl is immutable
        return (EntryImpl<E>) queue.poll();
    }
    
    @Override
    public void complete() {
        queue.updateSystemTs(END); //flush queue
        isComplete = true;
        onEvent(END);
    }

    @Override
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }
    
    @Override
    public BackPressureStrategy getStrategy() {
        return strategy;
    }
    
    @Override
    public String getId() {
        return id;
    }
    
    private void onEvent(long ts) {
        if (ts > system && ts >= graceExpiry) {
            hasChanged = true; //must come before write to 'system'
            system = ts;
            signalUpdate.run();
        }
        lastSystem = ts;
    }
    
}
