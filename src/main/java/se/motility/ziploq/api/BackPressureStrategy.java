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
     * Make producer thread wait until capacity
     * is available on associated queue.
     */
    BLOCK,
    
    /**
     * Drop new messages if capacity is
     * full on associated queue.
     */
    DROP;
    
}
