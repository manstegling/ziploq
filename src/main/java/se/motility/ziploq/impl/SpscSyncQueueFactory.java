/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.Comparator;
import java.util.Optional;

import se.motility.ziploq.api.BackPressureStrategy;

/**
 * Factory for creating instances of {@link SpscSyncQueue} 
 * to use in e.g. {@link ZiploqImpl} 
 * 
 * @author M Tegling
 *
 */
public interface SpscSyncQueueFactory {
    
    /**
     * Factory method for creating a {@link SpscSyncQueue} for unordered input
     * @param businessDelay maximum business time delay allowed for new messages, compared
     * to previous messages. Must not be greater than {@code systemDelay}.
     * @param systemDelay maximum amount of <i>system time</i> (wall-clock time; provided by
     * Producer) that any message can arrive late, compared to other messages with the exact
     * same business timestamp. Must be non-negative.
     * @param softCapacity of the queue; rounded up to the next power of 2 (if not already
     * power of 2). Messages having business timestamps in the last {@code businessDelay}
     * milliseconds won't count towards the total capacity.
     * @param comparator to use if multiple messages have the exact same business
     * timestamp. If {@link Optional#empty} is provided, no ordering is imposed on ties.
     * @param <E> message type
     * @return {@code SpscSyncQueue} to use with unordered input
     */
    public static <E> SpscSyncQueue<E> createUnordered(long businessDelay, long systemDelay,
            int softCapacity, BackPressureStrategy strategy, Optional<Comparator<E>> comparator) {
        ArgChecker.validateLong(businessDelay, 0, false, "businessDelay");
        ArgChecker.validateLong(businessDelay, systemDelay, true, "businessDelay");
        ArgChecker.validateLong(systemDelay, 0, false, "systemDelay");
        ArgChecker.validateLong(softCapacity, 1, false, "capacity");
        return strategy == BackPressureStrategy.UNBOUNDED
                ? UnboundedSyncQueue.unorderedSyncQueue(businessDelay, systemDelay, softCapacity, comparator)
                : new UnorderedSyncQueue<>(businessDelay, systemDelay, softCapacity, comparator);

    }
    
    /**
     * Factory method for creating a {@link SpscSyncQueue} for ordered input
     * @param capacity of the queue; rounded up to the next power of 2 (if not already power of 2) 
     * @param strategy 
     * @param <E> message type
     * @return {@code SpscSyncQueue} to use with ordered input
     */
    public static <E> SpscSyncQueue<E> createOrdered(int capacity, BackPressureStrategy strategy) {
        ArgChecker.validateLong(capacity, 1, false, "capacity");
        ArgChecker.notNull(strategy, "strategy");
        return strategy == BackPressureStrategy.UNBOUNDED
                ? UnboundedSyncQueue.orderedSyncQueue(capacity)
                : new OrderedSyncQueue<>(capacity);
    }
    
}
