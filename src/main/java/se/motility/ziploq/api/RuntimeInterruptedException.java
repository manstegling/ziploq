/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

/**
 * A runtime wrapper of {@link InterruptedException} to avoid checked exceptions
 * in pipeline code. Before this exception is thrown, the thread is re-interrupted.
 * <p>
 * Typical use-case:
 * <pre>
 * try {
 *     //code that may throw InterruptedException
 * } catch (InterruptedException e {
 *     Thread.currentThread().interrupt();
 *     throw new RuntimeInterruptedException("Thread interrupted", e);
 * }
 * </pre>
 * 
 * @author M Tegling
 *
 */
public class RuntimeInterruptedException extends RuntimeException {

    private static final long serialVersionUID = 987654312L;

    /**
     * Constructs an instance of this exception with a specified exception message
     * @param message about the exception
     */
    public RuntimeInterruptedException(String message) {
        super(message);
    }
    
    /**
     * Constructs an instance of this exception with a specified exception message
     * and its associated {@link InterruptedException}
     * @param message about the exception
     * @param cause the original InterruptedException
     */
    public RuntimeInterruptedException(String message, InterruptedException cause) {
        super(message, cause);
    }
    
} 
