/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class HiveConsumerFactoryTests extends OpenSearchTestCase {

    public void testCreateShardConsumer() {
        HiveConsumerFactory factory = new HiveConsumerFactory();

        IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.replication.type", "SEGMENT")
                    .put("index.ingestion_source.type", "HIVE")
                    .put("index.ingestion_source.pointer.init.reset", "earliest")
                    .put("index.ingestion_source.param.metastore_uri", "thrift://metastore:9083")
                    .put("index.ingestion_source.param.database", "test_db")
                    .put("index.ingestion_source.param.table", "events")
            )
            .build();

        HiveShardConsumer consumer = factory.createShardConsumer("client-1", 0, indexMetadata);
        assertNotNull(consumer);
        assertEquals(0, consumer.getShardId());
    }

    public void testParsePointerFromString() {
        HiveConsumerFactory factory = new HiveConsumerFactory();
        HivePointer pointer = factory.parsePointerFromString(
            "{\"p\":\"dt=2026-04-15\",\"f\":\"file:///data/part.parquet\",\"r\":42,\"s\":100}"
        );

        assertNotNull(pointer);
        assertEquals("dt=2026-04-15", pointer.getPartitionName());
        assertEquals("file:///data/part.parquet", pointer.getFilePath());
        assertEquals(42, pointer.getRowIndex());
        assertEquals(100, pointer.getSequenceNumber());
    }
}
