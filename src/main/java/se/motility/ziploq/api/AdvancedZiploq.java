/*
 * Copyright (c) 2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

import java.util.stream.Stream;

import se.motility.ziploq.impl.Splitr;
import se.motility.ziploq.impl.SyncQueue;
import se.motility.ziploq.impl.SyncQueueFactory;
import se.motility.ziploq.impl.WaitStrategy;
import se.motility.ziploq.impl.SyncQueueFactory.CapacityType;

/**
 * Utility class providing advanced tools for working with the Ziploq framework.
 * These tools can help increase performance for certain use-cases.
 * 
 * @author M Tegling
 *
 */
public final class AdvancedZiploq {
    
    /**
     * Similar to {@link Ziploq#stream} with the difference that sequencing is handled in
     * a separate thread and passed to the output stream via a lock-free buffer. This
     * removes the cost of sequencing--from a <i>throughput</i> perspective.
     * <p>
     * Use this if you need to maximize throughput and therefore cannot afford the cost of
     * sequencing inside the output stream. Depending on the number of input sources and
     * comparator used, the average cost may be everything from negligible, up to a couple
     * of microseconds per entry.
     * <p>
     * The worker thread will get the same thread priority as the thread calling this method.
     * @param ziploq to stream sequenced entries from
     * @param bufferSize of the worker's buffer
     * @param <E> message type
     * @return {@code Stream} consisting of synchronized messages
     * @throws RuntimeInterruptedException if thread is interrupted during wait
     * @implNote The worker thread spawns and starts filling the buffer immediately, rather
     * than waiting for a <i>terminal operation</i> to be invoked on the returned {@code Stream}
     */
    public static <E> Stream<Entry<E>> streamWithWorker(Ziploq<E> ziploq, int bufferSize) {
        SyncQueue<E> buffer = SyncQueueFactory.createOrdered(bufferSize, CapacityType.BOUNDED);
        new Thread(() -> transfer(ziploq, buffer), "ziploq-stream-worker").start(); //spawn on terminal op call instead?
        return Splitr.stream(() -> takeFrom(buffer), Ziploq.getEndSignal(), ziploq.getComparator());
    }
    
    
    private static <T> void transfer(Ziploq<T> ziploq, SyncQueue<T> buffer) {
        try {
            Entry<T> entry;
            while ((entry = ziploq.take()) != Ziploq.getEndSignal()) {
                buffer.put(entry);
            }
            buffer.put(entry);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeInterruptedException("Message transfer interrupted", e);
        }
    }
    
    private static <T> Entry<T> takeFrom(SyncQueue<T> buffer) throws InterruptedException {
        int attempt = 1;
        Entry<T> entry;
        while ((entry = buffer.poll()) == null) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Thread interrupted.");
            }
            WaitStrategy.backOffWait(attempt++);
        }
        return entry;
    }
    
    private AdvancedZiploq() {
        throw new UnsupportedOperationException("Invalid instantiation of utility class");
    }
    
}
