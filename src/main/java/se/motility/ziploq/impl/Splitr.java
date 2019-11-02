/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.RuntimeInterruptedException;

/**
 * An ordered spliterator capable of blocking.
 * <p>
 * Advances when new entries are made available and terminates once
 * the configured end signal is encountered.
 * 
 * @author M Tegling
 *
 * @param <E> message type
 */
public class Splitr<E> implements Spliterator<Entry<E>> {

    private final BlockingProducer<Entry<E>> producer;
    private final Comparator<Entry<E>> comparator;
    private final Entry<E> endSignal;
    
    /**
     * Wraps a blocking producer and transforms the ordered output into a stream of entries.
     * The stream may block when it's waiting to take new entries from the blocking producer.
     * The stream is terminated when the end signal is encountered.
     * @param producer source of entries
     * @param endSignal final entry to be produced; terminates the stream
     * @param comparator defining the message sequence coming from the blocking producer
     * @param <E> message type
     * @throws RuntimeInterruptedException if the thread is interrupted while taking
     * messages from the blocking producer
     */
    public static <E> Stream<Entry<E>> stream(
            BlockingProducer<Entry<E>> producer, Entry<E> endSignal, Comparator<Entry<E>> comparator) {
        return StreamSupport.stream(
                new Splitr<>(producer, endSignal, comparator), false);
    }
    
    /**
     * Constructs a spliterator to use with the Ziploq machinery
     * @param producer source of entries
     * @param endSignal final entry to be produced; terminates the stream
     * @param comparator defining the message sequence coming from the blocking producer
     */
    public Splitr(BlockingProducer<Entry<E>> producer, Entry<E> endSignal, Comparator<Entry<E>> comparator) {
        this.producer = producer;
        this.endSignal = endSignal;
        this.comparator = comparator;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Entry<E>> action) {
        Entry<E> entry;
        try {
            entry = producer.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeInterruptedException("Thread interrupted.", e);
        }
        if(entry != endSignal) {
            action.accept(entry);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Spliterator<Entry<E>> trySplit() {
        return null; //never split
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED | Spliterator.SORTED | Spliterator.NONNULL;
    }

    @Override
    public Comparator<Entry<E>> getComparator() {
        return comparator;
    }
    
    @FunctionalInterface
    public interface BlockingProducer<E> {
        E take() throws InterruptedException;
    }

}
