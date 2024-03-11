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
import se.motility.ziploq.impl.ManagedZiploqImpl;
import se.motility.ziploq.impl.ZiploqImpl;

/**
 * Factory for creating {@link Ziploq} and {@link ZipFlow} instances
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
     * ties when sequencing unordered input data. If {@code null} is provided, no ordering is imposed on ties.
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
     * ties when sequencing unordered input data. If {@code null} is provided, no ordering is imposed on ties.
     * @return a new {@code Ziploq} instance
     * @param <E> type of messages to be synchronized
     */
    static <E> Ziploq<E> create(Comparator<E> comparator) {
        return new ZiploqImpl<>(0, comparator);
    }

    /**
     * A builder for Ziploq providing dead-simple efficient job scheduling. Really useful when merging many more
     * high-volume sources than available threads on the machine, when maximum throughput is of essence.
     * @param comparator to use if multiple messages have the exact same business timestamp.
     * This comparator will also be used to determine ordering in instances of business timestamp
     * ties when sequencing unordered input data. If {@code null} is provided, no ordering is imposed on ties.
     * @param <E> type of messages to be synchronized
     * @return a new {@code ManagedZiploqBuilder} to configure a managed {@code Ziploq} instance
     */
    static <E> ManagedZiploqBuilder<E> managedZiploqBuilder(Comparator<E> comparator) {
        return new ManagedZiploqBuilder<>(comparator);
    }

    /**
     * A builder for ZipFlow providing dead-simple efficient job scheduling. Really useful when merging many more
     * high-volume sources than available threads on the machine, when maximum throughput is of essence.
     * <p>
     * Use this builder to pre-register all data sources of interest and then let a managed thread pool efficiently
     * retrieve, sort and merge data from the sources.
     * @param systemDelay maximum amount of <i>system time</i> (wall-clock time; provided by
     * Producers) that any message can arrive late, compared to other messages from the same
     * source having the exact same business timestamp. This is used in the heart-beating
     * mechanism to allow progression at times when certain sources are silent. This is also
     * the maximum drift allowed between system time and <i>business time</i>. Must be non-negative.
     * @param comparator to use if multiple messages have the exact same business timestamp.
     * This comparator will also be used to determine ordering in instances of business timestamp
     * ties when sequencing unordered input data. If {@code null} is provided, no ordering is imposed on ties.
     * @param <E> type of messages to be synchronized
     * @return a new {@code ManagedZipFlowBuilder} to configure a managed {@code ZipFlow} instance
     */
    static <E> ManagedZipFlowBuilder<E> managedZipFlowBuilder(long systemDelay, Comparator<E> comparator) {
        if(systemDelay <= 0) {
            throw new IllegalArgumentException("System delay must greater than 0. Provided value was " + systemDelay);
        }
        return new ManagedZipFlowBuilder<>(systemDelay, comparator);
    }

    /**
     * Fluent builder for setting up a 'Managed Ziploq' instance with ZipFlow functionality, automatically retrieving,
     * sorting and merging data efficiently from all registered sources in real-time using a fixed thread pool.
     *
     * @author M Tegling
     * @param <E> message type
     */
    class ManagedZipFlowBuilder<E> extends Builder<E, ManagedZipFlowBuilder<E>> {

        ManagedZipFlowBuilder(long systemDelay, Comparator<E> comparator) {
            super(systemDelay, comparator);
        }

        /**
         * Registers a new ordered input source to be synchronized.
         * <p>
         * The input data must form a non-decreasing sequence with respect to business timestamp.
         * Any ties must follow the ordering defined by the {@code ZipFlow} instance's configured
         * {@code Comparator}. If no {@code Comparator} has been provided, no ordering is imposed on ties.
         * @param source to register with the ZipFlow instance for managed data retrieval
         * @param businessDelay the maximum business time delay allowed for new messages, compared
         * to previous messages from the same source. Must not be greater than the configured
         * <i>system delay</i> of this {@code ZipFlow} instance
         * @param softCapacity of the buffer; rounded up to the next power of 2 (if not already
         * power of 2). Messages having business timestamps in the last {@code businessDelay}
         * milliseconds won't count towards the total capacity.
         * @param sourceName to be associated with this input source
         * @param comparator to use if messages from multiple queues have the exact same business
         * timestamp. If {@code null} is provided, no ordering is imposed on ties
         * @param <T> message type; must be a subclass of the synchronized type
         * @return this builder instance
         */
        public <T extends E> ManagedZipFlowBuilder<E> registerUnordered(
                FlowSource<T> source, long businessDelay, int softCapacity,
                String sourceName, Comparator<T> comparator) {
            FlowConsumer<T> consumer = ziploq.registerUnordered(businessDelay,
                    softCapacity, BackPressureStrategy.DROP, sourceName, comparator);
            addJob(Job.create(source, consumer));
            return this;
        }

        /**
         * Registers a new ordered input source to be synchronized.
         * <p>
         * The input data must form a non-decreasing sequence with respect to business timestamp.
         * Any ties must follow the ordering defined by the {@code ZipFlow} instance's configured
         * {@code Comparator}. If no {@code Comparator} has been provided, no ordering is imposed on ties.
         * @param source to register with the ZipFlow instance for managed data retrieval
         * @param capacity of the buffer; rounded up to the next power of 2 (if not already power of 2)
         * @param sourceName to be associated with this input source
         * @param <T> message type; must be a subclass of the synchronized type
         * @return this builder instance
         */
        public <T extends E> ManagedZipFlowBuilder<E> registerOrdered(
                FlowSource<T> source, int capacity, String sourceName) {
            FlowConsumer<T> consumer = ziploq.registerOrdered(capacity, BackPressureStrategy.DROP, sourceName);
            addJob(Job.create(source, consumer));
            return this;
        }

        @Override
        ManagedZipFlowBuilder<E> getThis() {
            return this;
        }

    }

    /**
     * Fluent builder for setting up a 'Managed Ziploq' instance, automatically retrieving, sorting and merging
     * data efficiently from all registered sources using a fixed thread pool.
     *
     * @author M Tegling
     * @param <E> message type
     */
    class ManagedZiploqBuilder<E> extends Builder<E, ManagedZiploqBuilder<E>> {

        ManagedZiploqBuilder(Comparator<E> comparator) {
            super(0L, comparator);
        }

        /**
         * Registers a new unordered input source to be synchronized.
         * <p>
         * The input data will be sorted with respect to business timestamp. Ordering of ties is
         * first resolved by this {@code Ziploq} instance's configured {@code Comparator}. Remaining
         * ties are then resolved by the {@code Comparator} provided when calling this method. If
         * no {@code Comparator} is provided, no ordering is imposed on remaining ties.
         * @param source to register with the Ziploq instance for managed data retrieval
         * @param businessDelay the maximum business time delay allowed for new messages, compared
         * to previous messages from the same source.
         * @param softCapacity of the buffer; rounded up to the next power of 2 (if not already
         * power of 2). Messages having business timestamps in the last {@code businessDelay}
         * milliseconds won't count towards the total capacity.
         * @param sourceName to be associated with this input source
         * @param comparator to use if messages from multiple queues have the exact same business
         * timestamp. If {@code null} is provided, no ordering is imposed on ties
         * @param <T> message type; must be a subclass of the synchronized type
         * @return this builder instance
         */
        public <T extends E> ManagedZiploqBuilder<E> registerUnordered(
                Source<T> source, long businessDelay, int softCapacity,
                String sourceName, Comparator<T> comparator) {
            FlowConsumer<T> consumer = ziploq.registerUnordered(businessDelay,
                    softCapacity, BackPressureStrategy.DROP, sourceName, comparator);
            addJob(Job.create(source, consumer));
            return this;
        }

        /**
         * Registers a new ordered input source to be synchronized.
         * <p>
         * The input data must form a non-decreasing sequence with respect to business timestamp.
         * Any ties must follow the ordering defined by this {@code Ziploq} instance's configured
         * {@code Comparator}. If no {@code Comparator} has been provided, no ordering is imposed on ties.
         * @param source to register with the Ziploq instance for managed data retrieval
         * @param capacity of the buffer; rounded up to the next power of 2 (if not already power of 2)
         * @param sourceName to be associated with this input source
         * @param <T> message type; must be a subclass of the synchronized type
         * @return {@link SynchronizedConsumer} to feed the input data into
         */
        public <T extends E> ManagedZiploqBuilder<E> registerOrdered(
                Source<T> source, int capacity, String sourceName) {
            FlowConsumer<T> consumer = ziploq.registerOrdered(capacity, BackPressureStrategy.DROP, sourceName);
            addJob(Job.create(source, consumer));
            return this;
        }

        @Override
        ManagedZiploqBuilder<E> getThis() {
            return this;
        }

    }

    abstract class Builder<E, B extends Builder<E, B>> {
        private final List<Job> jobs = new ArrayList<>();
        final ZipFlow<E> ziploq;
        private int poolSize = 3;

        public Builder(long systemDelay, Comparator<E> comparator) {
            this.ziploq = new ZiploqImpl<>(systemDelay, comparator);
        }

        /**
         * Defines the number of threads to use when retrieving, sorting and merging data from associated sources
         * @param poolSize number of threads
         * @return this builder instance
         */
        public B setPoolSize(int poolSize) {
            this.poolSize = poolSize;
            return getThis();
        }

        /**
         * Creates and starts a 'Managed Ziploq' instance
         * @return a started {@code ManagedZiploq} instance
         */
        public Zipq<E> create() {
            return new ManagedZiploqImpl<>(ziploq, jobs, poolSize);
        }

        abstract B getThis();

        void addJob(Job job) {
            jobs.add(job);
        }
    }

}
