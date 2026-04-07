/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;

/**
 * A virtual index input backed by block files downloaded from remote store.
 * Block files are a sequence of bytes of fixed block size belonging to a segment file.
 * Clone, slice, fetchBlock, and Builder will be added in the implementation PR.
 */
public class BlockIndexInput extends AbstractBlockIndexInput {

    /** The file name. */
    protected final String fileName;
    /** The size of the file. */
    protected final long fileSize;
    /** The underlying Lucene directory. */
    protected final FSDirectory directory;
    /** The IO context. */
    protected final IOContext context;

    // Placeholder constructor. Real constructor via Builder will be added in the implementation PR.
    BlockIndexInput(AbstractBlockIndexInput.Builder<?> builder) {
        super(builder);
        this.fileName = null;
        this.fileSize = 0;
        this.directory = null;
        this.context = null;
    }

    @Override
    public BlockIndexInput clone() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected IndexInput fetchBlock(int blockId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public BlockIndexInput buildSlice(String sliceDescription, long offset, long length) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
