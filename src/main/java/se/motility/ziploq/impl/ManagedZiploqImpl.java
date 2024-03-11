/*
 * Copyright (c) 2024 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.Comparator;
import java.util.List;

import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.api.Zipq;

/**
 * A facade for the Ziploq framework providing the implementation needed for 'Managed Ziploq'--the operation model
 * in which retrieving, sorting and merging data from multiple sources is fully managed by an efficient scheduler
 * using a thread pool.
 *
 * @author M Tegling
 *
 * @param <E> message type
 */
public class ManagedZiploqImpl<E> implements Zipq<E> {

    private final Ziploq<E> delegate;

    public ManagedZiploqImpl(Ziploq<E> delegate, List<Job> jobs, int threads) {
        JobScheduler scheduler = new JobScheduler(threads, jobs);
        scheduler.start();
        this.delegate = delegate;
    }

    @Override
    public Entry<E> take() throws InterruptedException {
        return delegate.take();
    }

    @Override
    public Entry<E> poll() {
        return delegate.poll();
    }

    @Override
    public Comparator<Entry<E>> getComparator() {
        return delegate.getComparator();
    }

    /**
     * Returns internal performance counters for debugging purposes
     * @return internal performance counters
     */
    public int[] delayStats() {
        return ((ZiploqImpl<E>) delegate).delayStats();
    }

}
