/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;

public class IngestionConsumerFactoryDefaultMethodsTests extends OpenSearchTestCase {

    /**
     * Minimal implementation that only implements required methods — verifies default methods work.
     */
    private static class MinimalFactory implements IngestionConsumerFactory<IngestionShardConsumer, IngestionShardPointer> {
        @Override
        public void initialize(IngestionSource ingestionSource) {}

        @Override
        public IngestionShardConsumer createShardConsumer(String clientId, int shardId) {
            return mock(IngestionShardConsumer.class);
        }

        @Override
        public IngestionShardPointer parsePointerFromString(String pointer) {
            return mock(IngestionShardPointer.class);
        }
    }

    public void testDefaultGetSourcePartitionCount() {
        MinimalFactory factory = new MinimalFactory();
        assertEquals(-1, factory.getSourcePartitionCount());
    }

    public void testDefaultCreateMultiPartitionShardConsumer() {
        MinimalFactory factory = new MinimalFactory();
        // Default falls back to createShardConsumer — should not throw
        IngestionShardConsumer consumer = factory.createMultiPartitionShardConsumer("client", 0, List.of(0, 1, 2));
        assertNotNull(consumer);
    }

    public void testDefaultCreateMultiPartitionShardConsumerIgnoresPartitionIds() {
        MinimalFactory factory = new MinimalFactory();
        // Default implementation ignores partitionIds and calls createShardConsumer(clientId, shardId)
        // So it should work with any partition list
        IngestionShardConsumer consumer1 = factory.createMultiPartitionShardConsumer("client", 0, List.of(5));
        IngestionShardConsumer consumer2 = factory.createMultiPartitionShardConsumer("client", 0, List.of(0, 4, 8, 12));
        assertNotNull(consumer1);
        assertNotNull(consumer2);
    }
}
