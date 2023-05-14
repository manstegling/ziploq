/*
 * Copyright (c) 2018 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.api;

/**
 * Container for message having an associated <i>business timestamp</i>
 * and <i>system timestamp</i>.
 * <p>
 * <i>Warning:</i> The {@code Entry} objects will only be {@code Serializable}
 * if type {@code T} is {@code Serializable}.
 * 
 * @author M Tegling
 *
 * @param <T> message type
 */
public interface Entry<T> extends BasicEntry<T> {

    /**
     * Returns the system timestamp (epoch) associated with this entry.
     * @return the system timestamp (epoch) associated with this entry.
     */
    long getSystemTs();
    
}
