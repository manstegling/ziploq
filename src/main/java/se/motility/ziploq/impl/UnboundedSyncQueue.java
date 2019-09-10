/*
 * Copyright (c) 2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.Comparator;
import java.util.Optional;

import se.motility.ziploq.api.Entry;

/**
 * Unbounded queue alternative; configured to handle either an ordered or
 * an unordered input sequence. For use in applications that cannot handle
 * back-pressure all the way through the application.
 * <p>
 * Signals when the desired maximum capacity has been reached--but does
 * <u>not</u> block or drop messages.
 * <p>
 * For performance reasons, capacity breaches might not be reported until
 * capacity has been exceeded with up to 127 messages.
 * 
 * @author M Tegling
 *
 * @param <E> message type
 * @see SpscSyncQueue
 */
public class UnboundedSyncQueue<E> implements SpscSyncQueue<E> {

    private static final int MASK = 127;
    
    private final SpscSyncQueue<E> delegate;
    private final int desiredCapacity;
    
    private int counter = 0;
    
    static <T> UnboundedSyncQueue<T> orderedSyncQueue(int capacity) {
        SpscSyncQueue<T> queue = new OrderedSyncQueue<>(0); //unbounded
        return new UnboundedSyncQueue<>(queue, capacity);
    }
    
    static <T> UnboundedSyncQueue<T> unorderedSyncQueue(long businessDelay,
            long systemDelay, int capacity, Optional<Comparator<T>> comparator) {
        SpscSyncQueue<T> queue = new UnorderedSyncQueue<>(
                businessDelay, systemDelay, Integer.MAX_VALUE, comparator);
        return new UnboundedSyncQueue<>(queue, capacity);
    }
    
    private UnboundedSyncQueue(SpscSyncQueue<E> delegate, int capacity) {
        this.delegate = delegate;
        this.desiredCapacity = capacity;
    }
    
    /**
     * Immediately puts entry on the unbounded queue and returns. Never blocks.
     * @return {@code true} if there's still space left until the desired
     * maximum capacity has been reached, {@code false} otherwise
     */
    @Override
    public boolean offer(Entry<E> entry) {
        if(!delegate.offer(entry)) {
            //Happens when queue reaches Integer.MAX_VALUE entries (fix this?)
            throw new IllegalStateException("Delegate does not accept new entry. Class: " 
                    + delegate.getClass());
        }
        return checkCapacity();
    }

    /**
     * Immediately puts entry on the unbounded queue and returns. Never blocks.
     * @return {@code true} if there's still space left until the desired
     * maximum capacity has been reached, {@code false} otherwise
     */
    @Override
    public boolean put(Entry<E> entry) {
        delegate.put(entry);
        return checkCapacity();
    }

    @Override
    public void updateSystemTs(long timestamp) {
        delegate.updateSystemTs(timestamp);
    }

    @Override
    public Entry<E> poll() {
        return delegate.poll();
    }

    @Override
    public Entry<E> peek() {
        return delegate.peek();
    }

    @Override
    public int size() {
        return delegate.size();
    }
    
    @Override
    public int readySize() {
        return delegate.readySize();
    }

    /**
     * Returns remaining capacity until the desired maximum capacity has
     * been reached. The value 0 means that the number of queued items go
     * beyond the desired maximum capacity.
     * @return number of entries the queue can accept until the maximum
     * desired capacity has been reached
     */
    @Override
    public int remainingCapacity() {
        return Math.max(0, desiredCapacity - readySize());
    }

    private boolean checkCapacity() {
        if ((counter++ & MASK) == 0) {
            return readySize() <= desiredCapacity;
        }
        return true;
    }
    
}
