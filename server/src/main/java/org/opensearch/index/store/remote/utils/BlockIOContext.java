/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.apache.lucene.store.IOContext;

/**
 * BlockIOContext is an extension of IOContext which can be used to pass block related information to the openInput() method of any directory
 */
public class BlockIOContext extends IOContext {

    private final boolean isBlockRequest;
    private long blockStart;
    private long blockSize;

    /**
     * Default constructor
     */
    BlockIOContext(IOContext ctx) {
        super(ctx.context);
        this.isBlockRequest = false;
        this.blockStart = -1;
        this.blockSize = -1;
    }

    /**
     * Constructor to initialise BlockIOContext with block related information
     */
    public BlockIOContext(IOContext ctx, long blockStart, long blockSize) {
        super(ctx.context);
        this.isBlockRequest = true;
        this.blockStart = blockStart;
        this.blockSize = blockSize;
    }

    /**
     * Function to check if the Context contains a block request or not
     */
    public boolean isBlockRequest() {
        return isBlockRequest;
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
}
