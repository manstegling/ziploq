/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

import java.util.Comparator;

/**
 * A device for synchronizing and sequencing messages from any number of input sources.
 * <p>
 * Each input source has to be individually registered with this device and configured
 * according to its nature. Both sources providing ordered and unordered data are
 * supported. See {@link #registerOrdered} and {@link #registerUnordered} for more information.
 * <p>
 * {@code ZipFlow} uses a two-clock system; the first clock being the <i>business
 * clock</i> and the second the <i>system clock</i> (wall-clock time). These two clocks are
 * assumed to be shared by all data sources (or at least not drifting outside the configured
 * safety-margin). All messages are sequenced with respect to the business clock, with the
 * possibility of providing a {@code Comparator} to resolve ordering of any ties. The system
 * clock is used to allow advancing the sequence at times when input sources are silent.
 * There are two parameters associated with message sequencing; <i>business delay</i> and
 * <i>system delay</i>.
 * <p>
 * Business delay is specified for unordered input sources and defines the upper bound on how
 * <i>late</i> messages may arrive in relation to other messages from the same source. In
 * particular, if an unordered source is configured with business delay {@code x} and the highest
 * business timestamp for a processed message so far is {@code tsMax}, the contract states that
 * only new messages having business timestamp {@code tsMax - x} or higher will be sequenced
 * correctly.
 * <p>
 * System delay specifies how much system time must pass until messages may be emitted at times
 * when input sources are silent. The {@code global system time} is defined by the input source
 * that has the lowest current system time. This guarantees that all input sources are up-to-date
 * and healthy (although potentially silent message-wise) before advancing the message sequence.
 * More formally; if one or more sources are silent, messages having a system timestamp higher
 * than {@code global system time + system delay} may be emitted. The system delay must be set to
 * a value higher than any configured business delay of registered unordered sources.
 * <p>
 * There are three methods for retrieving synchronized messages from this device; {@link #stream},
 * {@link #take} and {@link #poll}. For normal data processing purposes, building data pipelines
 * with {@code stream()} is encouraged.
 * <p>
 * The synchronization mechanism supports backpressure on both producer and consumer side. See
 * {@link BackPressureStrategy} for more information.
 * 
 * @author M Tegling
 *
 * @param <E> message type
 */
public interface ZipFlow<E> extends Ziploq<E> {
    
    /**
     * Registers a new unordered input source to be synchronized.
     * <p>
     * The input data will be sorted with respect to business timestamp. Ordering of ties is
     * first resolved by this {@code ZipFlow} instance's configured {@code Comparator}. Remaining
     * ties are then resolved by the {@code Comparator} provided when calling this method. If
     * no {@code Comparator} is provided, no ordering is imposed on remaining ties.
     * @param businessDelay the maximum business time delay allowed for new messages, compared
     * to previous messages from the same source. Must not be greater than the configured
     * <i>system delay</i> of this {@code ZipFlow} instance
     * @param softCapacity of the buffer; rounded up to the next power of 2 (if not already
     * power of 2). Messages having business timestamps in the last {@code businessDelay}
     * milliseconds won't count towards the total capacity.
     * @param strategy determining whether messages should be dropped ({@link
     * BackPressureStrategy#DROP}) when queues are full or if producer threads should have to
     * wait ({@link BackPressureStrategy#BLOCK}). There's also an option to use unbounded buffers
     * ({@link BackPressureStrategy#UNBOUNDED}).
     * @param sourceName to be associated with this input source
     * @param comparator to use if messages from multiple queues have the exact same business
     * timestamp. If {@code null} is provided, no ordering is imposed on ties
     * @param <T> message type; must be a subclass of the synchronized type
     * @return {@link FlowConsumer} to feed with input data
     */
    @Override
    <T extends E> FlowConsumer<T> registerUnordered(
                long businessDelay, int softCapacity, BackPressureStrategy strategy,
                String sourceName, Comparator<T> comparator);
    

    /**
     * Registers a new ordered input source to be synchronized.
     * <p>
     * The input data must form a non-decreasing sequence with respect to business timestamp.
     * Any ties must follow the ordering defined by this {@code ZipFlow} instance's configured
     * {@code Comparator}. If no {@code Comparator} has been provided, no ordering is imposed on ties.
     * @param capacity of the buffer; rounded up to the next power of 2 (if not already power of 2)
     * @param strategy determining whether messages should be dropped ({@link
     * BackPressureStrategy#DROP}) when queues are full or if producer threads should have to
     * wait ({@link BackPressureStrategy#BLOCK}). There's also an option to use unbounded buffers
     * ({@link BackPressureStrategy#UNBOUNDED}).
     * @param sourceName to be associated with this input source
     * @param <T> message type; must be a subclass of the synchronized type
     * @return {@link FlowConsumer} to feed the input data into
     */
    @Override
    <T extends E> FlowConsumer<T> registerOrdered(
                int capacity, BackPressureStrategy strategy, String sourceName);
    
    
}
