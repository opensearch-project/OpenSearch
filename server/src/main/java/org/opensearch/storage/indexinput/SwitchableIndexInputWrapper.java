/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import java.io.IOException;
import java.lang.ref.Cleaner;

public class SwitchableIndexInputWrapper extends FilterIndexInput implements RandomAccessInput {

    private final String resourceDescription;
    private final SwitchableIndexInput switchableIndexInput;
    private static final Logger logger = LogManager.getLogger(SwitchableIndexInputWrapper.class);
    private static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory("index-input-cleaner"));

    public SwitchableIndexInputWrapper(String resourceDescription, SwitchableIndexInput in) {
        super(resourceDescription, in);
        this.resourceDescription = resourceDescription;
        switchableIndexInput = in;
        CLEANER.register(this, switchableIndexInput);
    }

    @Override
    public IndexInput clone() {
        return new SwitchableIndexInputWrapper(
            "Cloned " + resourceDescription,
            switchableIndexInput.clone()
        );
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return switchableIndexInput.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
        return switchableIndexInput.readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        return switchableIndexInput.readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return switchableIndexInput.readLong(pos);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        this.switchableIndexInput.prefetch(offset, length);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        return new SwitchableIndexInputWrapper(
            "Sliced " + resourceDescription,
            switchableIndexInput.slice(sliceDescription, offset, length)
        );
    }

    @Override
    public void close() throws IOException {
        switchableIndexInput.close();
    }

    @Override
    public byte readByte() throws IOException {
        return switchableIndexInput.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return switchableIndexInput.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return switchableIndexInput.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return switchableIndexInput.readLong();
    }

    @Override
    public int readVInt() throws IOException {
        return switchableIndexInput.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        return switchableIndexInput.readVLong();
    }
}
