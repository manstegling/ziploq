/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

import java.util.Comparator;
import java.util.Optional;

import se.motility.ziploq.impl.ZiploqImpl;

/**
 * Factory for creating {@link Ziploq} instances
 * 
 * @author M Tegling
 *
 */
public interface ZiploqFactory {
    
    /**
     * Factory method for creating a {@link Ziploq}
     * @param systemDelay maximum amount of <i>system time</i> (wall-clock time; provided by
     * Producers) that any message can arrive late, compared to other messages from the same
     * source having the exact same business timestamp. This is also the maximum drift allowed
     * between system time and <i>business time</i>. Must be non-negative.
     * @param comparator to use if multiple messages have the exact same business timestamp.
     * This comparator will also be used to determine ordering in instances of business timestamp
     * ties when sequencing unordered input data. If {@link Optional#empty} is provided, no
     * ordering is imposed on ties.
     * @return a new {@code Ziploq} instance
     * @param <E> type of messages to be synchronized
     */
    public static <E> Ziploq<E> create(long systemDelay, Optional<Comparator<E>> comparator) {
        if(systemDelay < 0) {
            throw new IllegalArgumentException("System delay must be 0 or greater. Provided value was " + systemDelay);
        }
        return new ZiploqImpl<>(systemDelay, comparator);
    }
    
}
