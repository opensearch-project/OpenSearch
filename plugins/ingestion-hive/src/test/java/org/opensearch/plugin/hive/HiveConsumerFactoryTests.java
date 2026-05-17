/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class HiveConsumerFactoryTests extends OpenSearchTestCase {

    public void testCreateShardConsumer() {
        HiveConsumerFactory factory = new HiveConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://metastore:9083");
        params.put("database", "test_db");
        params.put("table", "events");
        params.put("_number_of_shards", 3);

        IngestionSource source = new IngestionSource.Builder("HIVE").setParams(params).build();
        HiveShardConsumer consumer = factory.createShardConsumer("client-1", 0, source);
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
