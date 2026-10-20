/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.IOException;
import java.util.Map;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowConsumerFactoryTests extends OpenSearchTestCase {

    private static IndexMetadata indexMetadataWithSource(String datasetType, int numShards) {
        return IndexMetadata.builder("test-index")
                .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                                .put(IndexMetadata.SETTING_INGESTION_SOURCE_TYPE, "ARROW")
                                .put("index.ingestion_source.param.dataset_type", datasetType)
                                .put("index.ingestion_source.param.sharding_key", "id"))
                .numberOfShards(numShards)
                .numberOfReplicas(0)
                .build();
    }

    public void testCreateShardConsumerDispatchesToRegisteredFactory() throws IOException {
        FakeArrowSourceFactory fakeFactory = new FakeArrowSourceFactory("FAKE");
        ArrowConsumerFactory consumerFactory = new ArrowConsumerFactory(Map.of("FAKE", fakeFactory));

        ArrowShardConsumer consumer = consumerFactory.createShardConsumer("client", 0, indexMetadataWithSource("fake", 2));

        assertNotNull(consumer);
        assertEquals(0, consumer.getShardId());
        assertEquals(1, fakeFactory.createCount);
    }

    public void testUnknownDatasetTypeThrows() {
        ArrowConsumerFactory consumerFactory = new ArrowConsumerFactory(Map.of("FAKE", new FakeArrowSourceFactory("FAKE")));
        expectThrows(
                IllegalArgumentException.class,
                () -> consumerFactory.createShardConsumer("client", 0, indexMetadataWithSource("other", 1)));
    }

    public void testParsePointerFromString() {
        ArrowConsumerFactory consumerFactory = new ArrowConsumerFactory(Map.of());
        ArrowOffset offset = consumerFactory.parsePointerFromString("55");
        assertEquals(55L, offset.getOffset());
    }

    private static final class FakeArrowSourceFactory implements ArrowSourceFactory {
        private final String type;
        int createCount = 0;

        FakeArrowSourceFactory(String type) {
            this.type = type;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public ArrowSource createArrowSource(Map<String, Object> params, ArrowIngestionConfig ingestionConfig) {
            createCount++;
            return new FakeArrowSource();
        }
    }

    private static final class FakeArrowSource implements ArrowSource {
        @Override
        public long getRowCount() {
            return 0;
        }

        @Override
        public org.apache.arrow.vector.ipc.ArrowReader getReader(long start, long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long getVersionTimestamp() {
            return null;
        }

        @Override
        public String getVersion() {
            return "fake";
        }

        @Override
        public void close() {}
    }
}
