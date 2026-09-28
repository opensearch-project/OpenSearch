/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class VirtualShardOperationRoutingTests extends OpenSearchTestCase {

    public void testGenerateVirtualShardId() {
        int numPhysicalShards = 4;
        int numVirtualShards = 16;

        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, numVirtualShards)
            )
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1)
            .build();

        String routing = "user1";
        int routingHash = Murmur3HashFunction.hash(routing);
        int expectedVShard = Math.floorMod(routingHash, numVirtualShards);
        int expectedPShard = expectedVShard / (numVirtualShards / numPhysicalShards);

        assertEquals(expectedPShard, OperationRouting.generateShardId(metadata, "doc1", routing));
    }

    public void testVirtualShardDisabledUsesLegacy() {
        int numPhysicalShards = 4;

        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, -1)
            )
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1)
            .build();

        String routing = "user1";
        int legacyHash = Murmur3HashFunction.hash(routing);
        int expectedLegacyShard = Math.floorMod(legacyHash, numPhysicalShards);

        assertEquals(expectedLegacyShard, OperationRouting.generateShardId(metadata, "doc1", routing));
    }

    public void testVirtualShardValidation() {
        int numPhysicalShards = 10;
        int numVirtualShards = 5;

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, numVirtualShards)
                )
                .numberOfShards(numPhysicalShards)
                .numberOfReplicas(1)
                .build()
        );

        assertTrue(e.getMessage().contains("must be >= number of shards"));

        int numVirtualShardsInvalid = 15;

        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test2")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, numVirtualShardsInvalid)
                )
                .numberOfShards(numPhysicalShards)
                .numberOfReplicas(1)
                .build()
        );

        assertTrue(e2.getMessage().contains("must be a multiple of number of shards"));
    }

    public void testVirtualShardWithRoutingPartition() {
        int numPhysicalShards = 4;
        int numVirtualShards = 16;
        int partitionSize = 2;

        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, numVirtualShards)
            )
            .routingPartitionSize(partitionSize)
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1)
            .build();

        String id = "doc1";
        String routing = "user1";

        int partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), partitionSize);
        int routingHash = Murmur3HashFunction.hash(routing) + partitionOffset;
        int expectedVShard = Math.floorMod(routingHash, numVirtualShards);
        int expectedPShard = expectedVShard / (numVirtualShards / numPhysicalShards);

        assertEquals(expectedPShard, OperationRouting.generateShardId(metadata, id, routing));
    }
}
