/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link ChecksumWritableBlobStoreFormat}
 */
public class ChecksumWritableBlobStoreFormatTests extends OpenSearchTestCase {
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long VERSION = 5L;

    private final ChecksumWritableBlobStoreFormat<IndexMetadata> clusterBlocksFormat = new ChecksumWritableBlobStoreFormat<>(
        "index-metadata",
        IndexMetadata::readFrom
    );

    public void testSerDe() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        BytesReference bytesReference = clusterBlocksFormat.serialize(indexMetadata, TEST_BLOB_FILE_NAME, CompressorRegistry.none());
        IndexMetadata readIndexMetadata = clusterBlocksFormat.deserialize(TEST_BLOB_FILE_NAME, bytesReference);
        assertThat(readIndexMetadata, is(indexMetadata));
    }

    public void testSerDeForCompressed() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        BytesReference bytesReference = clusterBlocksFormat.serialize(
            indexMetadata,
            TEST_BLOB_FILE_NAME,
            CompressorRegistry.getCompressor(DeflateCompressor.NAME)
        );
        IndexMetadata readIndexMetadata = clusterBlocksFormat.deserialize(TEST_BLOB_FILE_NAME, bytesReference);
        assertThat(readIndexMetadata, is(indexMetadata));
    }

    private IndexMetadata getIndexMetadata() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .version(VERSION)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
