/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import java.util.concurrent.locks.LockSupport;

public interface WaitStrategy {

    /**
     * Back-off wait strategy. Wait longer and longer
     * for each subsequent call (up to 1ms) .
     * @param attempts since last successful
     */
    public static void backOffWait(int attempts) {
        if (attempts <= 50) {
            Thread.yield();
        } else if(attempts == 51) {
            LockSupport.parkNanos(1L);
        } else if(attempts == 52) {
            LockSupport.parkNanos(10L);
        } else if (attempts == 53) {
            LockSupport.parkNanos(100L);
        } else if (attempts == 54) {
            LockSupport.parkNanos(1000L);
        } else if (attempts == 55) {
            LockSupport.parkNanos(10_000L);
        } else if (attempts == 56) {
            LockSupport.parkNanos(100_000L);
        } else {
            LockSupport.parkNanos(1_000_000L);
        }
    }
    
    /**
     * Wait for the specified amount of nanoseconds
     * @param nanos to wait
     */
    public static void specificWait(long nanos) {
        LockSupport.parkNanos(nanos);
    }
    
}
