/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * Wrapper around SwitchableIndexInput that implements RandomAccessInput.
 * Delegates all read operations to the underlying SwitchableIndexInput.
 * Clone, slice, RandomAccessInput methods, and Cleaner registration
 * will be added in the implementation PR.
 */
public class SwitchableIndexInputWrapper extends FilterIndexInput implements RandomAccessInput {

    private final SwitchableIndexInput switchableIndexInput;

    /**
     * Constructs a new SwitchableIndexInputWrapper.
     * @param resourceDescription the resource description
     * @param in the switchable index input
     */
    public SwitchableIndexInputWrapper(String resourceDescription, SwitchableIndexInput in) {
        super(resourceDescription, in);
        this.switchableIndexInput = in;
    }

    @Override
    public byte readByte(long pos) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public short readShort(long pos) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int readInt(long pos) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long readLong(long pos) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
