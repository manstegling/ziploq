/*
 * Copyright (c) 2018-2022 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

import java.util.Comparator;
import java.util.stream.Stream;

import se.motility.ziploq.impl.Splitr;

/**
 * The output side of Ziploq, i.e. an interface for retrieving the synchronized
 * stream of data. The interaction patterns are similar to a regular Queue.
 * <p>
 * There are three methods for retrieving sequenced messages from this device; {@link #stream},
 * {@link #take} and {@link #poll}. For normal data processing purposes, building data pipelines
 * with {@code stream()} is encouraged.
 * <p>
 * For the ability to register input sources use {@link Ziploq} or {@link ZipFlow}
 * as provided by the {@link ZiploqFactory}. For automatic threading and scheduling use
 * {@link ZiploqFactory#managedZiploqBuilder} or {@link ZiploqFactory#managedZipFlowBuilder}, respectively.
 *
 * @author M Tegling
 *
 * @param <E> message type
 */
public interface Zipq<E> {

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
        return Splitr.stream(this::take, Ziploq.getEndSignal(), getComparator());
    }

    /**
     * Retrieves synchronized message. Waits if necessary for a message to become available.
     * After all associated {@link SynchronizedConsumer} instances have completed and all messages
     * have been taken, a special end marker entry is emitted (see {@link Ziploq#getEndSignal}).
     * @return a synchronized message wrapped in an {@link Entry}, or {@link Ziploq#getEndSignal}
     * if all input sources have completed
     * @throws InterruptedException if thread is interrupted during wait
     */
    Entry<E> take() throws InterruptedException;

    /**
     * Retrieves synchronized message. This method will return immediately even if no message is
     * available. After all associated {@link SynchronizedConsumer} instances have completed and
     * all messages have been taken, a special end marker entry is emitted (see {@link Ziploq#getEndSignal}).
     * <p>
     * <i>Note:</i> If dropping messages is not allowed; rather than calling this method
     * in a busy-spin fashion, use method {@link #take()}. Repeatedly calling
     * this method may cause thread contention.
     * @return a synchronized message wrapped in an {@link Entry}, or {@link Ziploq#getEndSignal}
     * all input sources have completed
     */
    Entry<E> poll();

    /**
     * Returns the effective {@code Comparator} used for sequencing messages from the associated
     * {@link SynchronizedConsumer} instances.
     * @return effective comparator used to sequence input data
     */
    Comparator<Entry<E>> getComparator();

}
