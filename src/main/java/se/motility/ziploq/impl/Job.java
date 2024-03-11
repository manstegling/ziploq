/*
 * Copyright (c) 2022-2023 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.function.Supplier;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.BasicEntry;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.FlowSource;
import se.motility.ziploq.api.Source;
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;

/**
 * A {@code Job} encapsulates a source in a way that the developer does not need to implement logic for retrieving
 * messages from the source and insert into the Ziploq framework. In essence, {@code Job} is the adapter for
 * transforming from a pull-based model ({@link FlowSource}) to the reactive model represented by 'Managed Ziploq'.
 *
 * @author M Tegling
 */
public class Job {

    enum State {
        READY, WAITING, BLOCKED, COMPLETED;
    }

    private final Supplier<State> callable;

    private Job(Supplier<State> callable) {
        this.callable = callable;
    }

    public static <T> Job create(Source<T> source, SynchronizedConsumer<T> consumer) {
        if (consumer.getStrategy() != BackPressureStrategy.DROP) {
            throw new IllegalArgumentException("The scheduling framework requires a DROP-based consumer");
        }
        EntryContainer<T, BasicEntry<T>> entryContainer = new EntryContainer<>();
        return new Job(() -> invocation(source, consumer, entryContainer));
    }

    public static <T> Job create(FlowSource<T> source, FlowConsumer<T> consumer) {
        if (consumer.getStrategy() != BackPressureStrategy.DROP) {
            throw new IllegalArgumentException("The scheduling framework requires a DROP-based consumer");
        }
        EntryContainer<T, Entry<T>> entryContainer = new EntryContainer<>();
        return new Job(() -> invocation(source, consumer, entryContainer));
    }

    private static <T> State invocation(
            Source<T> source, SynchronizedConsumer<T> consumer, EntryContainer<T, BasicEntry<T>> current){
        if (current.entry == null) {
            current.entry = source.emit();
        }
        BasicEntry<T> e = current.entry;
        if (e == null) { // waiting for upstream
            return State.WAITING;
        } else if (e == Ziploq.getEndSignal()) { // no more messages
            consumer.complete();
            return State.COMPLETED;
        } else { // message emitted, may or may not fit into downstream
            boolean accepted = consumer.onEvent(e.getMessage(), e.getBusinessTs());
            if (accepted) {
                current.entry = null;
                return State.READY;
            } else {
                return State.BLOCKED;
            }
        }
    }

    private static <T> State invocation(
            FlowSource<T> source, FlowConsumer<T> consumer, EntryContainer<T, Entry<T>> current){
        if (current.entry == null) {
            current.entry = source.emit();
        }
        Entry<T> e = current.entry;
        if (e == null) { // waiting for upstream
            consumer.updateSystemTime(source.getSystemTs());
            return State.WAITING;
        } else if (e == Ziploq.getEndSignal()) { // no more messages
            consumer.complete();
            return State.COMPLETED;
        } else { // message emitted, may or may not fit into downstream
            boolean accepted = consumer.onEvent(e.getMessage(), e.getBusinessTs(), e.getSystemTs());
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

    private static class EntryContainer<T, E extends BasicEntry<T>> {
        private E entry;
    }

}
