/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.shards.ShardCounts;
import org.opensearch.common.ValidationException;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.MetadataIndexStateServiceTests.addClosedIndex;
import static org.opensearch.cluster.metadata.MetadataIndexStateServiceTests.addOpenedIndex;
import static org.opensearch.cluster.shards.ShardCounts.forDataNodeCount;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_IGNORE_DOT_INDEXES;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_MAX_SHARDS_PER_CLUSTER_KEY;

public class ShardLimitValidatorTests extends OpenSearchTestCase {

    public void testOverShardLimit() {
        int nodesInCluster = randomIntBetween(1, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);

        Settings clusterSettings = Settings.builder().build();

        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas());

        int shardsToAdd = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(shardsToAdd, state, counts.getShardsPerNode(), -1);

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        assertTrue(errorMessage.isPresent());
        assertEquals(
            "this action would add ["
                + totalShards
                + "] total shards, but this cluster currently has ["
                + currentShards
                + "]/["
                + maxShards
                + "] maximum shards open",
            errorMessage.get()
        );
    }

    public void testOverShardLimitWithMaxShardCountLimit() {
        int nodesInCluster = randomIntBetween(1, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);

        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas());
        int shardsToAdd = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int maxShardLimitOnCluster = shardsToAdd - 1;
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(
            shardsToAdd,
            state,
            counts.getShardsPerNode(),
            maxShardLimitOnCluster
        );

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = Math.min(counts.getShardsPerNode() * nodesInCluster, maxShardLimitOnCluster);
        assertTrue(errorMessage.isPresent());
        assertEquals(
            "this action would add ["
                + totalShards
                + "] total shards, but this cluster currently has ["
                + currentShards
                + "]/["
                + maxShards
                + "] maximum shards open",
            errorMessage.get()
        );
    }

    public void testUnderShardLimit() {
        int nodesInCluster = randomIntBetween(2, 90);
        // Calculate the counts for a cluster 1 node smaller than we have to ensure we have headroom
        ShardCounts counts = forDataNodeCount(nodesInCluster - 1);

        Settings clusterSettings = Settings.builder().build();

        ClusterState state = createClusterForShardLimitTest(nodesInCluster, counts.getFirstIndexShards(), counts.getFirstIndexReplicas());

        int existingShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int shardsToAdd = randomIntBetween(1, (counts.getShardsPerNode() * nodesInCluster) - existingShards);
        Optional<String> errorMessage = ShardLimitValidator.checkShardLimit(shardsToAdd, state, counts.getShardsPerNode(), -1);

        assertFalse(errorMessage.isPresent());
    }

    /**
     * This test validates that system index creation succeeds
     * even though it exceeds the cluster max shard limit
     */
    public void testSystemIndexCreationSucceeds() {
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(1, false);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        shardLimitValidator.validateShardLimit(".tasks", settings, state);
    }

    /**
     * This test validates that non-system index creation
     * fails when it exceeds the cluster max shard limit
     */
    public void testNonSystemIndexCreationFails() {
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(1, false);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        final ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit("abc", settings, state)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + 2
                + "] total shards, but this cluster currently has ["
                + 1
                + "]/["
                + 1
                + "] maximum shards open;",
            exception.getMessage()
        );
    }

    public void testNonSystemIndexCreationFailsWithMaxShardLimitOnCluster() {
        final int maxShardLimitOnCluster = 1;
        Settings limitOnlySettings = Settings.builder()
            .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 1)
            .put(SETTING_CLUSTER_IGNORE_DOT_INDEXES.getKey(), false)
            .put(SETTING_MAX_SHARDS_PER_CLUSTER_KEY, maxShardLimitOnCluster)
            .build();
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(limitOnlySettings);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        final ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit("abc", settings, state)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + 2
                + "] total shards, but this cluster currently has ["
                + 1
                + "]/["
                + maxShardLimitOnCluster
                + "] maximum shards open;",
            exception.getMessage()
        );
    }

    public void testNonSystemIndexCreationPassesWithMaxShardLimitOnCluster() {
        final int maxShardLimitOnCluster = 5;
        Settings limitOnlySettings = Settings.builder()
            .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 3)
            .put(SETTING_CLUSTER_IGNORE_DOT_INDEXES.getKey(), false)
            .put(SETTING_MAX_SHARDS_PER_CLUSTER_KEY, maxShardLimitOnCluster)
            .build();
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(limitOnlySettings);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        shardLimitValidator.validateShardLimit("abc", settings, state);
    }

    /**
     * This test validates that index starting with dot creation Succeeds
     * when the setting cluster.ignore_dot_indexes is set to true.
     */
    public void testDotIndexCreationSucceeds() {
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(1, true);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        shardLimitValidator.validateShardLimit(".test-index", settings, state);
    }

    /**
     * This test validates that index starting with dot creation fails
     * when the setting cluster.ignore_dot_indexes is set to false.
     */
    public void testDotIndexCreationFails() {
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(1, false);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        final ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(".test-index", settings, state)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + 2
                + "] total shards, but this cluster currently has ["
                + 1
                + "]/["
                + 1
                + "] maximum shards open;",
            exception.getMessage()
        );
    }

    /**
     * This test validates that dataStream index creation fails
     * when the cluster.ignore_dot_indexes is set to true, and we reach the max shard per node limit.
     */
    public void testDataStreamIndexCreationFails() {
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(1, true);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        final ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(".ds-test-index", settings, state)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + 2
                + "] total shards, but this cluster currently has ["
                + 1
                + "]/["
                + 1
                + "] maximum shards open;",
            exception.getMessage()
        );
    }

    /**
     * This test validates that dataStream index creation succeeds
     * when the cluster.ignore_dot_indexes is set to true, and we don't reach the max shard per node limit.
     */
    public void testDataStreamIndexCreationSucceeds() {
        final ShardLimitValidator shardLimitValidator = createTestShardLimitService(4, true);
        final Settings settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        final ClusterState state = createClusterForShardLimitTest(1, 1, 0);
        shardLimitValidator.validateShardLimit(".ds-test-index", settings, state);
    }

    /**
     * This test validates that non-system index opening
     * fails when it exceeds the cluster max shard limit
     */
    public void testNonSystemIndexOpeningFails() {
        int nodesInCluster = randomIntBetween(2, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);
        ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas()
        );

        Index[] indices = Arrays.stream(state.metadata().indices().values().toArray(new IndexMetadata[0]))
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode(), false);
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(state, indices)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + totalShards
                + "] total shards, but this cluster currently has ["
                + currentShards
                + "]/["
                + maxShards
                + "] maximum shards open;",
            exception.getMessage()
        );
    }

    /**
     * This test validates that system index opening succeeds
     * even when it exceeds the cluster max shard limit
     */
    public void testSystemIndexOpeningSucceeds() {
        int nodesInCluster = randomIntBetween(2, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);
        ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            randomAlphaOfLengthBetween(5, 15),
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            ".tasks",               // Adding closed system index to cluster state
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas()
        );

        Index[] indices = Arrays.stream(state.metadata().indices().values().toArray(new IndexMetadata[0]))
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        // Shard limit validation succeeds without any issues as system index is being opened
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode(), false);
        shardLimitValidator.validateShardLimit(state, indices);
    }

    /**
     * This test validates that index having '.' in the first character
     * opening of such indexes succeeds even when it exceeds the cluster max shard limit if the
     * cluster.ignore_dot_indexes setting is set to true.
     */
    public void testDotIndexOpeningSucceeds() {
        int nodesInCluster = randomIntBetween(2, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);
        ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            randomAlphaOfLengthBetween(5, 15),
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            ".test-index",               // Adding closed index starting with dot to cluster state
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas()
        );

        Index[] indices = Arrays.stream(state.metadata().indices().values().toArray(new IndexMetadata[0]))
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        // Shard limit validation succeeds without any issues
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode(), true);
        shardLimitValidator.validateShardLimit(state, indices);
    }

    /**
     * This test validates that index having '.' in the first character
     * opening fails when it exceeds the cluster max shard limit if the
     * cluster.ignore_dot_indexes is set to false.
     */
    public void testDotIndexOpeningFails() {
        int nodesInCluster = randomIntBetween(2, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);
        ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            randomAlphaOfLengthBetween(5, 15),
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            ".test-index",               // Adding closed index starting with dot to cluster state
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas()
        );

        Index[] indices = Arrays.stream(state.metadata().indices().values().toArray(new IndexMetadata[0]))
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode(), false);
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(state, indices)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + totalShards
                + "] total shards, but this cluster currently has ["
                + currentShards
                + "]/["
                + maxShards
                + "] maximum shards open;",
            exception.getMessage()
        );
    }

    /**
     * This test validates that index starting with '.ds-'
     * opening fails when it exceeds the cluster max shard limit if the
     * cluster.ignore_dot_indexes is set to true.
     */
    public void testDataStreamIndexOpeningFails() {
        int nodesInCluster = randomIntBetween(2, 90);
        ShardCounts counts = forDataNodeCount(nodesInCluster);
        ClusterState state = createClusterForShardLimitTest(
            nodesInCluster,
            randomAlphaOfLengthBetween(5, 15),
            counts.getFirstIndexShards(),
            counts.getFirstIndexReplicas(),
            ".ds-test-index",               // Adding closed data stream index to cluster state
            counts.getFailingIndexShards(),
            counts.getFailingIndexReplicas()
        );

        Index[] indices = Arrays.stream(state.metadata().indices().values().toArray(new IndexMetadata[0]))
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toList())
            .toArray(new Index[2]);

        int totalShards = counts.getFailingIndexShards() * (1 + counts.getFailingIndexReplicas());
        int currentShards = counts.getFirstIndexShards() * (1 + counts.getFirstIndexReplicas());
        int maxShards = counts.getShardsPerNode() * nodesInCluster;
        ShardLimitValidator shardLimitValidator = createTestShardLimitService(counts.getShardsPerNode(), true);
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> shardLimitValidator.validateShardLimit(state, indices)
        );
        assertEquals(
            "Validation Failed: 1: this action would add ["
                + totalShards
                + "] total shards, but this cluster currently has ["
                + currentShards
                + "]/["
                + maxShards
                + "] maximum shards open;",
            exception.getMessage()
        );
    }

    public static ClusterState createClusterForShardLimitTest(int nodesInCluster, int shardsInIndex, int replicas) {
        final Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5, 15), mock(DiscoveryNode.class));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes);

        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(5, 15))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .creationDate(randomLong())
            .numberOfShards(shardsInIndex)
            .numberOfReplicas(replicas);
        Metadata.Builder metadata = Metadata.builder().put(indexMetadata);
        if (randomBoolean()) {
            metadata.transientSettings(Settings.EMPTY);
        } else {
            metadata.persistentSettings(Settings.EMPTY);
        }

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).nodes(nodes).build();
    }

    public static ClusterState createClusterForShardLimitTest(
        int nodesInCluster,
        String openIndexName,
        int openIndexShards,
        int openIndexReplicas,
        String closeIndexName,
        int closedIndexShards,
        int closedIndexReplicas
    ) {
        final Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        for (int i = 0; i < nodesInCluster; i++) {
            dataNodes.put(randomAlphaOfLengthBetween(5, 15), mock(DiscoveryNode.class));
        }
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getDataNodes()).thenReturn(dataNodes);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        state = addOpenedIndex(openIndexName, openIndexShards, openIndexReplicas, state);
        state = addClosedIndex(closeIndexName, closedIndexShards, closedIndexReplicas, state);

        final Metadata.Builder metadata = Metadata.builder(state.metadata());
        if (randomBoolean()) {
            metadata.persistentSettings(Settings.EMPTY);
        } else {
            metadata.transientSettings(Settings.EMPTY);
        }
        return ClusterState.builder(state).metadata(metadata).nodes(nodes).build();
    }

    public static ClusterState createClusterForShardLimitTest(
        int nodesInCluster,
        int openIndexShards,
        int openIndexReplicas,
        int closedIndexShards,
        int closedIndexReplicas
    ) {
        return createClusterForShardLimitTest(
            nodesInCluster,
            randomAlphaOfLengthBetween(5, 15),
            openIndexShards,
            openIndexReplicas,
            randomAlphaOfLengthBetween(5, 15),
            closedIndexShards,
            closedIndexReplicas
        );
    }

    /**
     * Creates a {@link ShardLimitValidator} for testing with the given setting and a mocked cluster service.
     *
     * @param limitOnlySettings the setting used for creating ShardLimitValidator.
     * @return a test instance
     */
    private static ShardLimitValidator createTestShardLimitService(final Settings limitOnlySettings) {
        // Use a mocked clusterService - for unit tests we won't be updating the setting anyway.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(limitOnlySettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        return new ShardLimitValidator(limitOnlySettings, clusterService, new SystemIndices(emptyMap()));
    }

    /**
     * Creates a {@link ShardLimitValidator} for testing with the given setting and a mocked cluster service.
     *
     * @param maxShardsPerNode the value to use for the max shards per node setting
     * @param ignoreDotIndexes validates if index starting with dot should be ignored or not
     * @return a test instance
     */
    public static ShardLimitValidator createTestShardLimitService(int maxShardsPerNode, boolean ignoreDotIndexes) {
        // Use a mocked clusterService - for unit tests we won't be updating the setting anyway.
        ClusterService clusterService = mock(ClusterService.class);
        Settings limitOnlySettings = Settings.builder()
            .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode)
            .put(SETTING_CLUSTER_IGNORE_DOT_INDEXES.getKey(), ignoreDotIndexes)
            .build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(limitOnlySettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        return new ShardLimitValidator(limitOnlySettings, clusterService, new SystemIndices(emptyMap()));
    }

    /**
     * Creates a {@link ShardLimitValidator} for testing with the given setting and a given cluster service.
     *
     * @param maxShardsPerNode the value to use for the max shards per node setting
     * @param ignoreDotIndexes validates if index starting with dot should be ignored or not
     * @param clusterService   the cluster service to use
     * @return a test instance
     */
    public static ShardLimitValidator createTestShardLimitService(
        int maxShardsPerNode,
        boolean ignoreDotIndexes,
        ClusterService clusterService
    ) {
        Settings limitOnlySettings = Settings.builder()
            .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode)
            .put(SETTING_CLUSTER_IGNORE_DOT_INDEXES.getKey(), ignoreDotIndexes)
            .build();

        return new ShardLimitValidator(limitOnlySettings, clusterService, new SystemIndices(emptyMap()));
    }
}
