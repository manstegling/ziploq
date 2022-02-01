/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

import java.util.Comparator;

import se.motility.ziploq.impl.ZiploqImpl;

/**
 * Factory for creating {@link Ziploq} and {link ZipFlow} instances
 * 
 * @author M Tegling
 *
 */
public interface ZiploqFactory {
    
    /**
     * Factory method for creating a {@link ZipFlow}
     * <p>
     * Use this if not all data is yet available and you need to be able to push data through
     * the pipe even when some input sources are silent.
     * @param systemDelay maximum amount of <i>system time</i> (wall-clock time; provided by
     * Producers) that any message can arrive late, compared to other messages from the same
     * source having the exact same business timestamp. This is used in the heart-beating
     * mechanism to allow progression at times when certain sources are silent. This is also
     * the maximum drift allowed between system time and <i>business time</i>. Must be non-negative.
     * @param comparator to use if multiple messages have the exact same business timestamp.
     * This comparator will also be used to determine ordering in instances of business timestamp
     * ties when sequencing unordered input data. If {@code null} is provided, no
     * ordering is imposed on ties.
     * @return a new {@code ZipFlow} instance
     * @param <E> type of messages to be synchronized
     */
    static <E> ZipFlow<E> create(long systemDelay, Comparator<E> comparator) {
        if(systemDelay <= 0) {
            throw new IllegalArgumentException("System delay must greater than 0. Provided value was " + systemDelay);
        }
        return new ZiploqImpl<>(systemDelay, comparator);
    }
    
    /**
     * Factory method for creating a {@link Ziploq}
     * <p>
     * Use this if either 
     * <ol type="A">
     *   <li>all data is already available for sequencing, or</li>
     *   <li>you need to sequence all data with no upper bound on how late data may arrive</li>
     * </ol>
     * @param comparator to use if multiple messages have the exact same business timestamp.
     * This comparator will also be used to determine ordering in instances of business timestamp
     * ties when sequencing unordered input data. If {@code null} is provided, no
     * ordering is imposed on ties.
     * @return a new {@code Ziploq} instance
     * @param <E> type of messages to be synchronized
     */
    static <E> Ziploq<E> create(Comparator<E> comparator) {
        return new ZiploqImpl<>(0, comparator);
    }
    
}
