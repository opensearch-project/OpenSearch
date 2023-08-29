/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.core.util.Throwables;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.ExecutionException;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class CodecCompressionLevelIT extends OpenSearchIntegTestCase {

    public void testLuceneCodecsCreateIndexWithCompressionLevel() {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

        // creating index
        assertThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                    .put("index.codec.compression_level", randomIntBetween(1, 6))
                    .build()
            )
        );

        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                .build()
        );
        ensureGreen(index);
    }

    public void testZStandardCodecsCreateIndexWithCompressionLevel() {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(CodecService.ZSTD_CODEC, CodecService.ZSTD_NO_DICT_CODEC))
                .put("index.codec.compression_level", randomIntBetween(1, 6))
                .build()
        );

        ensureGreen(index);
    }

    public void testZStandardToLuceneCodecsWithCompressionLevel() throws ExecutionException, InterruptedException {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(CodecService.ZSTD_CODEC, CodecService.ZSTD_NO_DICT_CODEC))
                .put("index.codec.compression_level", randomIntBetween(1, 6))
                .build()
        );
        ensureGreen(index);

        assertAcked(client().admin().indices().prepareClose(index));

        Throwable executionException = expectThrows(
            ExecutionException.class,
            () -> client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder().put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                    )
                )
                .get()
        );

        Throwable rootCause = Throwables.getRootCause(executionException);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertTrue(rootCause.getMessage().startsWith("Compression level cannot be set"));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                            .put("index.codec.compression_level", (String) null)
                    )
                )
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index));
        ensureGreen(index);
    }

    public void testLuceneToZStandardCodecsWithCompressionLevel() throws ExecutionException, InterruptedException {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                .build()
        );
        ensureGreen(index);

        assertAcked(client().admin().indices().prepareClose(index));

        Throwable executionException = expectThrows(
            ExecutionException.class,
            () -> client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                            .put("index.codec.compression_level", randomIntBetween(1, 6))
                    )
                )
                .get()
        );

        Throwable rootCause = Throwables.getRootCause(executionException);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertTrue(rootCause.getMessage().startsWith("Compression level cannot be set"));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(CodecService.ZSTD_CODEC, CodecService.ZSTD_NO_DICT_CODEC))
                            .put("index.codec.compression_level", randomIntBetween(1, 6))
                    )
                )
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index));
        ensureGreen(index);
    }

}
