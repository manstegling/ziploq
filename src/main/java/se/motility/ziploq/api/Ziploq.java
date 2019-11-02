/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

import se.motility.ziploq.impl.Splitr;

/**
 * A device for synchronizing and sequencing messages from any number of input sources.
 * <p>
 * Each input source has to be individually registered with this device and configured
 * according to its nature. Both sources providing ordered and unordered data are
 * supported. See {@link #registerOrdered} and {@link #registerUnordered} for more information.
 * <p>
 * {@code Ziploq} relies on a global business clock. All messages are sequenced with respect to
 * the this business clock, with the possibility of providing a {@code Comparator} to resolve
 * ordering of any ties.
 * <p>
 * For unordered input sources the parameter <i>business delay</i> defines the upper bound on how
 * <i>late</i> messages may arrive in relation to other messages from the same source. In
 * particular, if an unordered source is configured with business delay {@code x} and the highest
 * business timestamp for a processed message so far is {@code tsMax}, the contract states that
 * only new messages having business timestamp {@code tsMax - x} or higher will be sequenced
 * correctly.
 * <p>
 * There are three methods for retrieving sequenced messages from this device; {@link #stream},
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
public interface Ziploq<E> {
    
    /**
     * Returns a special marker {@code Entry} indicating that all consumers have been
     * de-registered and no further messages will be provided. This message is used to
     * signal termination by both {@link #take()} and {@link #poll()} methods.
     * <p>
     * In the standard case, receiving this message means that the thread taking/polling
     * from this {@code Ziploq} should move on and terminate. For example:
     * <pre>
     * Entry&lt;E&gt; entry;
     * while ((entry = ziploq.take()) != Ziploq.getEndSignal()) {
     *     //handle entry
     * }
     * </pre>
     * @param <E> message type of the messages in the {@code Ziploq}
     * @return a special {@code Entry} indicating that all consumers have been
     * de-registered and no further messages will be provided
     */
    @SuppressWarnings("unchecked")
    public static <E> Entry<E> getEndSignal() {
        return (Entry<E>) END_SIGNAL;
    }
    
    /**
     * Returns a {@code Stream} consisting of synchronized messages. The length of this
     * {@code Stream} is undefined; messages will be provided until all associated
     * {@link SynchronizedConsumer} instances have completed. Until completed, the stream
     * will <i>block</i> the thread when awaiting new messages to emit.
     * <p>
     * Use this method to build a data pipeline with backpressure based on synchronized messages.
     * @return {@code Stream} consisting of synchronized messages
     * @throws RuntimeInterruptedException if thread is interrupted during wait
     */
    default Stream<Entry<E>> stream() {
        return Splitr.stream(this::take, getEndSignal(), getComparator());
    }
    
    /**
     * Retrieves synchronized message. Waits if necessary for a message to become available.
     * After all associated {@link SynchronizedConsumer} instances have completed and all messages
     * have been taken, a special end marker entry is emitted (see {@link #getEndSignal}).
     * @return a synchronized message wrapped in an {@link Entry}
     * @throws InterruptedException if thread is interrupted during wait
     */
    Entry<E> take() throws InterruptedException;
    
    /**
     * Retrieves synchronized message. This method will return immediately even if no message is
     * available. After all associated {@link SynchronizedConsumer} instances have completed and
     * all messages have been taken, a special end marker entry is emitted (see {@link #getEndSignal}).
     * <p>
     * <i>Note:</i> If dropping messages is not allowed; rather than calling this method
     * in a busy-spin fashion, use method {@link #take()}. Repeatedly calling
     * this method may cause thread contention.
     * @return a synchronized message wrapped in an {@link Entry}
     */
    Entry<E> poll();
    
    /**
     * Registers a new unordered input source to be synchronized.
     * <p>
     * The input data will be sorted with respect to business timestamp. Ordering of ties is
     * first resolved by this {@code Ziploq} instance's configured {@code Comparator}. Remaining
     * ties are then resolved by the {@code Comparator} provided when calling this method. If
     * no {@code Comparator} is provided, no ordering is imposed on remaining ties.
     * @param businessDelay the maximum business time delay allowed for new messages, compared
     * to previous messages from the same source.
     * @param softCapacity of the buffer; rounded up to the next power of 2 (if not already
     * power of 2). Messages having business timestamps in the last {@code businessDelay}
     * milliseconds won't count towards the total capacity.
     * @param strategy determining whether messages should be dropped ({@link
     * BackPressureStrategy#DROP}) when queues are full or if producer threads should have to
     * wait ({@link BackPressureStrategy#BLOCK}). There's also an option to use unbounded buffers
     * ({@link BackPressureStrategy#UNBOUNDED}).
     * @param sourceName to be associated with this input source
     * @param comparator to use if messages from multiple queues have the exact same business
     * timestamp. If {@link Optional#empty} is provided, no ordering is imposed on ties
     * @param <T> message type; must be a subclass of the synchronized type
     * @return {@link SynchronizedConsumer} to feed with input data
     */
    <T extends E> SynchronizedConsumer<T> registerUnordered(
                long businessDelay, int softCapacity, BackPressureStrategy strategy,
                String sourceName, Optional<Comparator<T>> comparator);
    
    /**
     * Registers a new ordered input source to be synchronized.
     * <p>
     * The input data must form a non-decreasing sequence with respect to business timestamp.
     * Any ties must follow the ordering defined by this {@code Ziploq} instance's configured
     * {@code Comparator}. If no {@code Comparator} has been provided, no ordering is imposed on ties.
     * @param capacity of the buffer; rounded up to the next power of 2 (if not already power of 2)
     * @param strategy determining whether messages should be dropped ({@link
     * BackPressureStrategy#DROP}) when queues are full or if producer threads should have to
     * wait ({@link BackPressureStrategy#BLOCK}). There's also an option to use unbounded buffers
     * ({@link BackPressureStrategy#UNBOUNDED}).
     * @param sourceName to be associated with this input source
     * @param <T> message type; must be a subclass of the synchronized type
     * @return {@link SynchronizedConsumer} to feed the input data into
     */
    <T extends E> SynchronizedConsumer<T> registerOrdered(
                int capacity, BackPressureStrategy strategy, String sourceName);
 
    /**
     * Returns the effective {@code Comparator} used for sequencing messages from the associated
     * {@link SynchronizedConsumer} instances.
     * @return effective comparator used to sequence input data
     */
    Comparator<Entry<E>> getComparator();
    
    /**
     * The end marker {@code Entry}. This {@code Entry} is immutable and serializable.
     *
     * @see #getEndSignal()
     **/
    @SuppressWarnings("rawtypes")
    static final Entry END_SIGNAL = new Entry() {
        private static final long serialVersionUID = 1L;
        @Override
        public Object getMessage() {
            return null;
        }
        @Override
        public long getBusinessTs() {
            return Long.MAX_VALUE;
        }
        @Override
        public long getSystemTs() {
            return Long.MAX_VALUE;
        }
    };
    
}
