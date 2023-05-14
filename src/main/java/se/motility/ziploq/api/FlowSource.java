/*
 * Copyright (c) 2022-2023 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

/**
 * When a Source is to be used with managed ZipFlow we need to be able to retrieve the current time of the source.
 * Provides same thread-safety guarantees as basic Source.
 * @param <E> message type
 *
 * @author M Tegling
 */
public interface FlowSource<E> extends Source<E> {

    /**.
     * Emits a new entry containing a message, its associated business timestamp and the current system timestamp
     * for the source. If there presently are no more messages available, but the source is still active, {@code null}
     * is emitted. If there are no more messages available and the source has completed, {@link Ziploq#getEndSignal()}
     * is emitted.
     * @return Entry containing message, business timestamp and system timestamp
     * {@code null} if no more messages are available at present but more will come, or {@code Ziploq#getEndSignal()}
     * if {@code Source} has completed.
     */
    @Override
    Entry<E> emit();

    /**
     * Returns the current system timestamp associated with the source
     * @return current system timestamp
     */
    long getSystemTs();

}
