/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

/**
 * Consumer associated with a {@link Ziploq}. Submit messages from an 
 * input source to this consumer to synchronize with messages from other sources.
 * <p>
 * Single-thread access only.
 * 
 * @author M Tegling
 *
 * @param <E> message type accepted by the consumer
 */
public interface SynchronizedConsumer<E> {
    
    /**
     * Submits the provided message to the {@code Ziploq} machinery.
     * <p>
     * If the underlying queue is full, the thread may either <i>block</i> until capacity
     * is available or <i>drop the message</i>, depending on configuration of the
     * consumer. All submitted events will advance the consumer's internal vector clock,
     * even if dropped.
     * <p>
     * For ordered input sources, business time must be updated in a non-decreasing sequence.
     * For unordered input sources, events can only be late by (at most) the configured
     * <i>business delay</i> for the sequencing mechanism to guarantee correct sequencing.
     * System time must always be updated in a non-decreasing sequence.
     * @param message to synchronize
     * @param businessTs business timestamp (epoch)
     * @return {@code true} if event was successfully added,
     * {@code false} if dropped or added but capacity was reached (see
     * {@link BackPressureStrategy#UNBOUNDED})
     * @throws RuntimeInterruptedException if thread is interrupted during wait
     * (blocking consumers only)
     * @throws IllegalStateException if called after {@link #complete} has been called
     */
    boolean onEvent(E message, long businessTs);
    
    /**
     * This will send a signal to the associated {@link Ziploq} to de-register this
     * consumer after all currently enqueued messages have been processed. Call when
     * no more events will be added. 
     * <p>
     * After this method has been called, no further calls to {@link #onEvent} or
     * {@link #updateSystemTime} are allowed.
     */
    void complete();
    
    /**
     * Returns the number of additional messages the consumer currently can accept
     * without having to exercise its back-pressure strategy. Since the downstream
     * most probably is busy processing messages, the number may change at any time.
     * <p>
     * Please note that calling this can be significantly slower than {@link #onEvent}.
     * If you think you have to call this once per message, please re-consider what
     * {@link BackPressureStrategy} you intend to use.
     * @return the number of additional messages the consumer currently can accept
     */
    int remainingCapacity();
    
    /**
     * Returns the backpressure strategy associated with the consumer
     * @return {@link BackPressureStrategy#BLOCK}, {@link BackPressureStrategy#DROP}
     * or {@link BackPressureStrategy#UNBOUNDED}
     */
    BackPressureStrategy getStrategy();
    
    /**
     * Returns the ID assigned to the consumer. The ID is unique among
     * all instances of {@code SynchronizedConsumer} in the JVM.
     * @return ID assigned to this consumer
     */
    String getId();
    
}
