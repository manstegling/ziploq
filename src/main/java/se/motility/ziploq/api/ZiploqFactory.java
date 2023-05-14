/*
 * Copyright (c) 2018-2023 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import se.motility.ziploq.impl.Job;
import se.motility.ziploq.impl.JobScheduler;
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

    /**
     *
     * @param comparator
     * @param <E>
     * @return
     */
    static <E> ManagedZiploqBuilder<E> managedZiploqBuilder(Comparator<E> comparator) {
        return new ManagedZiploqBuilder<>(comparator);
    }

    /**
     *
     * @param systemDelay
     * @param comparator
     * @param <E>
     * @return
     */
    static <E> ManagedZipFlowBuilder<E> managedZipFlowBuilder(long systemDelay, Comparator<E> comparator) {
        if(systemDelay <= 0) {
            throw new IllegalArgumentException("System delay must greater than 0. Provided value was " + systemDelay);
        }
        return new ManagedZipFlowBuilder<>(systemDelay, comparator);
    }

    /**
     *
     * @param <E>
     */
    class ManagedZipFlowBuilder<E> extends Builder<E, ManagedZipFlowBuilder<E>> {

        ManagedZipFlowBuilder(long systemDelay, Comparator<E> comparator) {
            super(systemDelay, comparator);
        }

        @Override
        ManagedZipFlowBuilder<E> getThis() {
            return this;
        }

        public <T extends E> ManagedZipFlowBuilder<E> registerUnordered(
                FlowSource<T> source, long businessDelay, int softCapacity,
                String sourceName, Comparator<T> comparator) {
            FlowConsumer<T> consumer = ziploq.registerUnordered(businessDelay,
                    softCapacity, BackPressureStrategy.DROP, sourceName, comparator);
            addJob(Job.create(source, consumer, true));
            return this;
        }

        public <T extends E> ManagedZipFlowBuilder<E> registerOrdered(
                FlowSource<T> source, int capacity, String sourceName) {
            FlowConsumer<T> consumer = ziploq.registerOrdered(capacity, BackPressureStrategy.DROP, sourceName);
            addJob(Job.create(source, consumer, true));
            return this;
        }

    }

    /**
     *
     * @param <E>
     */
    class ManagedZiploqBuilder<E> extends Builder<E, ManagedZiploqBuilder<E>> {

        ManagedZiploqBuilder(Comparator<E> comparator) {
            super(0L, comparator);
        }

        @Override
        ManagedZiploqBuilder<E> getThis() {
            return this;
        }

        public <T extends E> ManagedZiploqBuilder<E> registerUnordered(
                Source<T> source, long businessDelay, int softCapacity,
                String sourceName, Comparator<T> comparator) {
            FlowConsumer<T> consumer = ziploq.registerUnordered(businessDelay,
                    softCapacity, BackPressureStrategy.DROP, sourceName, comparator);
            addJob(Job.create(wrap(source), consumer, false));
            return this;
        }

        public <T extends E> ManagedZiploqBuilder<E> registerOrdered(
                Source<T> source, int capacity, String sourceName) {
            FlowConsumer<T> consumer = ziploq.registerOrdered(capacity, BackPressureStrategy.DROP, sourceName);
            addJob(Job.create(wrap(source), consumer, false));
            return this;
        }
        
        private static <T> FlowSource<T> wrap(Source<T> source) {
            return new FlowSource<T>() {
                @Override
                public long getSystemTs() {
                    return -1L;
                }
                @Override
                public Entry<T> emit() {
                    BasicEntry<T> e = source.emit();
                    return e == Ziploq.getEndSignal() ? (Entry<T>) e : new BasicEntryWrapper<>(e);
                }
            };
        }
    }

    abstract class Builder<E, B extends Builder<E, B>> {
        private final List<Job> jobs = new ArrayList<>();
        final ZipFlow<E> ziploq;
        private int poolSize = 3;

        public Builder(long systemDelay, Comparator<E> comparator) {
            this.ziploq = new ZiploqImpl<>(systemDelay, comparator);
        }

        public B setPoolSize(int poolSize) {
            this.poolSize = poolSize;
            return getThis();
        }

        public Zipq<E> create() {
            JobScheduler scheduler3 = new JobScheduler(poolSize, jobs);
            scheduler3.start();
            return ziploq;
        }

        abstract B getThis();

        void addJob(Job job) {
            jobs.add(job);
        }
    }

    class BasicEntryWrapper<T> implements Entry<T> {
        private final BasicEntry<T> entry;
        BasicEntryWrapper(BasicEntry<T> entry) {
            this.entry = entry;
        }
        @Override
        public T getMessage() {
            return entry.getMessage();
        }
        @Override
        public long getBusinessTs() {
            return entry.getBusinessTs();
        }
        @Override
        public long getSystemTs() {
            return -1L;
        }
    }

}
