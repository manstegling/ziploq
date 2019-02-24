/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

public interface ArgChecker {

    /**
     * Validate that provided value is on the right side of the threshold
     * @param value to validate
     * @param threshold for validation (inclusive)
     * @param below {@code true} if value should be less than or equal to threshold,
     * {@code false} if value should be greater than or equal to threshold
     * @param argName to show in exception if value is not valid
     */
    public static void validateLong(long value, long threshold, boolean below, String argName) {
        if(below ? (value > threshold) : (value < threshold)) {
            throw new IllegalArgumentException("Argument '" + argName + " must be " + 
                    threshold + " or " + (below ? "smaller " : "greater ") + ". Value is " + value);
        }
    }
    
    /**
     * Validate that provided value is not {@code null}
     * @param value to validate
     * @param argName to show in exception if value is not valid
     */
    public static void notNull(Object value, String argName) {
        if (value == null) {
           throw new IllegalArgumentException("Argument '" + argName + "' is null."); 
        }
    }
    
}
