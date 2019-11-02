/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

/**
 * Available strategies for handling backpressure
 *
 * @author M Tegling
 */
public enum BackPressureStrategy {

    /**
     * Makes producer thread wait until capacity
     * is available on associated queue.
     */
    BLOCK,
    
    /**
     * Drops new messages if capacity is
     * full on associated queue.
     */
    DROP,
    
    /**
     * No back-pressure is exercised but signals
     * when desired max capacity has been reached.
     */
    UNBOUNDED;
    
}
