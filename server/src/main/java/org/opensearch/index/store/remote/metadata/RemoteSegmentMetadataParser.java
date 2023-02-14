/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.metadata.MetadataParser;

/**
 * Parser for {@link RemoteSegmentMetadata}
 */
public class RemoteSegmentMetadataParser implements MetadataParser<RemoteSegmentMetadata> {
    /**
     * Reads metadata content from metadata file input stream and parsed into {@link RemoteSegmentMetadata}
     * @param indexInput metadata file input stream with {@link IndexInput#getFilePointer()} pointing to metadata content
     * @return {@link RemoteSegmentMetadata}
     * @throws IOException
     */
    @Override
    public RemoteSegmentMetadata readContent(IndexInput indexInput) throws IOException {
        return RemoteSegmentMetadata.fromMapOfStrings(indexInput.readMapOfStrings());
    }

    /**
     * Writes metadata to file output stream
     * @param indexOutput metadata file input stream
     * @param content {@link RemoteSegmentMetadata} from which metadata content would be generated
     * @throws IOException
     */
    @Override
    public void writeContent(IndexOutput indexOutput, RemoteSegmentMetadata content) throws IOException {}

    /**
     * Writes metadata to file output stream
     * @param indexOutput metadata file input stream
     * @param content metadata content
     * @throws IOException
     *
     * This method would be removed in future
     * and only {@link #writeContent(IndexOutput, RemoteSegmentMetadata)} would be leveraged in future
     */
    @Override
    public void writeContent(IndexOutput indexOutput, Map<String, String> content) throws IOException {
        indexOutput.writeMapOfStrings(content);
    }
}
