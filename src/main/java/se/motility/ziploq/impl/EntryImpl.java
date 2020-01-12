/*
 * Copyright (c) 2018-2019 Måns Tegling
 * 
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

import se.motility.ziploq.api.Entry;

/**
 * Simple implementation of {@link Entry}.
 * <p>
 * Includes a "hidden" reference to the Ziploq consumer to which the messages was submitted,
 * which is required by {@link ZiploqImpl} for performance reasons.
 * 
 * @author M Tegling
 *
 * @param <T> message type
 */
public class EntryImpl<T> implements Entry<T> {
    
    private static final long serialVersionUID = 2131688610740365735L;
    
    private final T item;
    private final long businessTs;
    private final long systemTs;
    private final transient FlowConsumerImpl<T> _queueRef;

    EntryImpl(T item, long businessTs, long systemTs, FlowConsumerImpl<T> queueRef) {
        this.item = item;
        this.businessTs = businessTs;
        this.systemTs = systemTs;
        this._queueRef = queueRef;
    }

    @Override
    public long getBusinessTs() {
        return businessTs;
    }

    @Override
    public long getSystemTs() {
        return systemTs;
    }

    @Override
    public T getMessage() {
        return item;
    }
    
    FlowConsumerImpl<T> getQueueRef() {
        return _queueRef;
    }

    @Override
    public String toString() {
        return "businessTs: " + businessTs +
               ", systemTs: " + systemTs +
               ", message: " + item;
    }
}
