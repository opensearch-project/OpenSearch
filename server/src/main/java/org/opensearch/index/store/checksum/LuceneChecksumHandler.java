/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.VerifyingIndexOutput;

import java.io.IOException;

/**
 * Checksum strategy for Lucene segment files.
 *
 * <p>Reads the checksum from the Lucene codec footer — an O(1) operation
 * since it only reads the last 16 bytes of the file.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class LuceneChecksumHandler implements FormatChecksumStrategy {

    @Override
    public long computeChecksum(Directory dir, String fileName) throws IOException {
        try (IndexInput input = dir.openInput(fileName, IOContext.READONCE)) {
            return CodecUtil.retrieveChecksum(input);
        }
    }

    @Override
    public VerifyingIndexOutput createVerifyingOutput(StoreFileMetadata metadata, IndexOutput output) {
        return new Store.LuceneVerifyingIndexOutput(metadata, output);
    }
}
