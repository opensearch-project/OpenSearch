/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import java.io.IOException;
import java.lang.ref.Cleaner;

/**
 * Wrapper around {@link FormatSwitchableIndexInput} that uses a {@link Cleaner}
 * to ensure resources are released even if the caller forgets to close.
 *
 * <p>Same pattern as {@link SwitchableIndexInputWrapper} — the cleaner holds a
 * reference to the inner {@link FormatSwitchableIndexInput} (which implements
 * {@link Runnable}). When this wrapper becomes phantom-reachable, the cleaner
 * invokes {@code run()} → {@code close()} on the inner input.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatSwitchableIndexInputWrapper extends FilterIndexInput implements RandomAccessInput {

    private static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory("index-input-cleaner"));

    private final String resourceDescription;
    private final FormatSwitchableIndexInput formatSwitchableIndexInput;

    public FormatSwitchableIndexInputWrapper(String resourceDescription, FormatSwitchableIndexInput in) {
        super(resourceDescription, in);
        this.resourceDescription = resourceDescription;
        this.formatSwitchableIndexInput = in;
        CLEANER.register(this, in);
    }

    /**
     * Returns the underlying {@link FormatSwitchableIndexInput} for switch operations.
     */
    public FormatSwitchableIndexInput unwrap() {
        return formatSwitchableIndexInput;
    }

    @Override
    public IndexInput clone() {
        return new FormatSwitchableIndexInputWrapper("Cloned " + resourceDescription, formatSwitchableIndexInput.clone());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        IndexInput sliced = formatSwitchableIndexInput.slice(sliceDescription, offset, length);
        if (sliced instanceof FormatSwitchableIndexInput) {
            return new FormatSwitchableIndexInputWrapper("Sliced " + resourceDescription, (FormatSwitchableIndexInput) sliced);
        }
        return sliced;
    }

    @Override
    public void close() throws IOException {
        formatSwitchableIndexInput.close();
    }

    @Override
    public byte readByte() throws IOException {
        return formatSwitchableIndexInput.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return formatSwitchableIndexInput.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return formatSwitchableIndexInput.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return formatSwitchableIndexInput.readLong();
    }

    @Override
    public int readVInt() throws IOException {
        return formatSwitchableIndexInput.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        return formatSwitchableIndexInput.readVLong();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        formatSwitchableIndexInput.readBytes(b, offset, len);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return formatSwitchableIndexInput.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
        return formatSwitchableIndexInput.readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        return formatSwitchableIndexInput.readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return formatSwitchableIndexInput.readLong(pos);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        formatSwitchableIndexInput.prefetch(offset, length);
    }
}
