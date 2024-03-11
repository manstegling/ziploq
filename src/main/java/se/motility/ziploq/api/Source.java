/*
 * Copyright (c) 2022-2023 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

/**
 * A formulation of the input source such that managed threads can retrieve new entries when suitable.
 * Even though there might be different threads accessing consecutive entries, all necessary memory
 * visibility guarantees, etc. are in place, so don't worry about thread-safety. Only beware that
 * ThreadLocals won't work.
 *
 * @author M Tegling
 *
 * @param <E> message type
 */
public interface Source<E> {

    /**
     * Emits a new entry containing a message and its associated business timestamp. If there presently are no
     * more messages available, but the source is still active, {@code null} is emitted. If there are no more
     * messages available and the source has completed, {@link Ziploq#getEndSignal()} is emitted.
     * @return Entry containing message, business timestamp and system timestamp
     * {@code null} if no more messages are available at present but more will come, or {@code Ziploq#getEndSignal()}
     * if {@code Source} has completed.
     */
    BasicEntry<E> emit();

}
