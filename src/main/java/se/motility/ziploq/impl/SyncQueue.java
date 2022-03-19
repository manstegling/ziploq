/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.RuntimeInterruptedException;

/**
 * Multi-producer, single-consumer queue interface used in implementation
 * 
 * @author M Tegling
 *
 * @param <E> message type
 */
public interface SyncQueue<E> {
    
    /**
     * Tries to enqueue provided {@link Entry} to the queue. If queue has no more
     * capacity, it will <i>drop</i> the entry. For queues with an internal vector clock,
     * enqueuing an entry will advance the clock, even if entry is being dropped.
     * @param entry to enqueue
     * @return {@code true} if entry was enqueued successfully, or {@code false} if dropped
     */
    boolean offer(Entry<E> entry);
    
    /**
     * Enqueue provided {@link Entry} to the queue. If queue has no more
     * capacity, it will <i>block</i> until capacity is available.
     * @param entry to enqueue
     * @return {@code true}
     * @throws RuntimeInterruptedException if blocking thread was interrupted
     */
    boolean put(Entry<E> entry);
 
    /**
     * Updates last seen system timestamp. May release held messages in queues with
     * sorting functionality. Not applicable for other queues (no-op).
     * @param timestamp system timestamp (epoch)
     */
    void updateSystemTs(long timestamp);
    
    /**
     * Retrieves and removes the head of the queue, or returns {@code null} if the
     * queue is empty.
     * @return head of the queue if available, otherwise {@code null}
     */
    Entry<E> poll();
    
    /**
     * Retrieves the head of the queue, or returns {@code null} if the queue is empty.
     * Does not remove the head from the queue.
     * @return the head of the queue, or {@code null} if the queue is empty
     */
    Entry<E> peek();
    
    /**
     * Returns the size of the queue, including any messages that are not yet sequenced
     * @return the size of the queue, including any messages that are not yet sequenced
     */
    int size();
    
    /**
     * Returns the number of entries available for polling. For a queue with sorting
     * functionality this will be less than or equal to the total {@link #size()} 
     * @return the number of entries available for polling
     */
    int readySize();
    
    /**
     * Returns the additional number of messages this queue can accept given capacity constraints
     * @return the additional number of messages this queue can accept
     */
    int remainingCapacity();
    
}
