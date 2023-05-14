/*
 * Copyright (c) 2022-2023 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.function.Supplier;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.FlowSource;
import se.motility.ziploq.api.Ziploq;

public class Job {

    enum State {
        READY, WAITING, BLOCKED, COMPLETED;
    }

    private final Supplier<State> callable;

    private Job(Supplier<State> callable) {
        this.callable = callable;
    }

    public static <T> Job create(FlowSource<T> source, FlowConsumer<T> consumer, boolean useFlow) {
        if (consumer.getStrategy() != BackPressureStrategy.DROP) {
            throw new IllegalArgumentException("The scheduling framework requires a DROP-based consumer");
        }
        EntryContainer<T> entryContainer = new EntryContainer<>();
        return new Job(() -> invocation(source, consumer, entryContainer, useFlow));
    }

    private static <T> State invocation(
            FlowSource<T> source, FlowConsumer<T> consumer, EntryContainer<T> current, boolean useFlow){
        if (current.entry == null) {
            current.entry = source.emit();
        }
        Entry<T> e = current.entry;
        if (e == null) { // waiting for upstream
            if (useFlow) {
                consumer.updateSystemTime(source.getSystemTs());
            }
            return State.WAITING;
        } else if (e == Ziploq.getEndSignal()) { // no more messages
            consumer.complete();
            return State.COMPLETED;
        } else { // message emitted, may or may not fit into downstream
            boolean accepted = useFlow
                    ? consumer.onEvent(e.getMessage(), e.getBusinessTs(), e.getSystemTs())
                    : consumer.onEvent(e.getMessage(), e.getBusinessTs());
            if (accepted) {
                current.entry = null;
                return State.READY;
            } else {
                return State.BLOCKED;
            }
        }
    }

    public State invoke() {
        return callable.get();
    }

    private static class EntryContainer<T> {
        private Entry<T> entry;
    }

}
