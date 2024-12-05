/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.apache.lucene.store.IOContext;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * BlockIOContext is an extension of IOContext which can be used to pass block related information to the openInput() method of any directory
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class BlockIOContext extends IOContext {

    private long blockStart;
    private long blockSize;

    /**
     * Constructor to initialise BlockIOContext with block related information
     */
    public BlockIOContext(IOContext ctx, long blockStart, long blockSize) {
        super(ctx.context);
        verifyBlockStartAndSize(blockStart, blockSize);
        this.blockStart = blockStart;
        this.blockSize = blockSize;
    }

    /**
     * Getter for blockStart
     */
    public long getBlockStart() {
        return blockStart;
    }

    /**
     * Getter for blockSize
     */
    public long getBlockSize() {
        return blockSize;
    }

    private void verifyBlockStartAndSize(long blockStart, long blockSize) {
        if (blockStart < 0) throw new IllegalArgumentException("blockStart must be greater than or equal to 0");
        if (blockSize <= 0) throw new IllegalArgumentException(("blockSize must be greater than 0"));
    }
}
