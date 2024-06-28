/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.indices.IndicesService.CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_SETTING_REPLICATION_TYPE;
import static org.hamcrest.Matchers.hasSize;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationClusterSettingIT extends OpenSearchIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    private static final String SYSTEM_INDEX_NAME = ".test-system-index";
    protected static final int SHARD_COUNT = 1;
    protected static final int REPLICA_COUNT = 1;

    protected static final String REPLICATION_MISMATCH_VALIDATION_ERROR =
        "Validation Failed: 1: index setting [index.replication.type] is not allowed to be set as [cluster.index.restrict.replication.type=true];";

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testIndexReplicationSettingOverridesSegRepClusterSetting() throws Exception {
        Settings settings = Settings.builder().put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
        final String ANOTHER_INDEX = "test-index";

        // Starting two nodes with primary and replica shards respectively.
        final String primaryNode = internalCluster().startNode(settings);
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to override cluster replication setting by passing a index replication setting
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
        ).get();
        createIndex(ANOTHER_INDEX);
        ensureYellowAndNoInitializingShards(INDEX_NAME, ANOTHER_INDEX);
        final String replicaNode = internalCluster().startNode(settings);

        // Randomly close and open index.
        if (randomBoolean()) {
            logger.info("--> Closing the index ");
            client().admin().indices().prepareClose(INDEX_NAME).get();

            logger.info("--> Opening the index");
            client().admin().indices().prepareOpen(INDEX_NAME).get();
        }
        ensureGreen(INDEX_NAME, ANOTHER_INDEX);

        final GetSettingsResponse response = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME, ANOTHER_INDEX).includeDefaults(true))
            .actionGet();
        assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.DOCUMENT.toString());
        assertEquals(response.getSetting(ANOTHER_INDEX, SETTING_REPLICATION_TYPE), ReplicationType.SEGMENT.toString());

        // Verify index setting isSegRepEnabled.
        Index index = resolveIndex(INDEX_NAME);
        Index anotherIndex = resolveIndex(ANOTHER_INDEX);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, primaryNode);
        assertEquals(indicesService.indexService(index).getIndexSettings().isSegRepEnabledOrRemoteNode(), false);
        assertEquals(indicesService.indexService(anotherIndex).getIndexSettings().isSegRepEnabledOrRemoteNode(), true);
    }

    public void testIndexReplicationSettingOverridesDocRepClusterSetting() throws Exception {
        Settings settings = Settings.builder().put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build();
        final String ANOTHER_INDEX = "test-index";
        final String primaryNode = internalCluster().startNode(settings);
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to override cluster replication setting by passing a index replication setting
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        ).get();
        createIndex(ANOTHER_INDEX);
        ensureYellowAndNoInitializingShards(INDEX_NAME, ANOTHER_INDEX);
        final String replicaNode = internalCluster().startNode(settings);
        ensureGreen(INDEX_NAME, ANOTHER_INDEX);

        final GetSettingsResponse response = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME, ANOTHER_INDEX).includeDefaults(true))
            .actionGet();
        assertEquals(response.getSetting(INDEX_NAME, SETTING_REPLICATION_TYPE), ReplicationType.SEGMENT.toString());
        assertEquals(response.getSetting(ANOTHER_INDEX, SETTING_REPLICATION_TYPE), ReplicationType.DOCUMENT.toString());

        // Verify index setting isSegRepEnabled.
        Index index = resolveIndex(INDEX_NAME);
        Index anotherIndex = resolveIndex(ANOTHER_INDEX);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, primaryNode);
        assertEquals(indicesService.indexService(index).getIndexSettings().isSegRepEnabledOrRemoteNode(), true);
        assertEquals(indicesService.indexService(anotherIndex).getIndexSettings().isSegRepEnabledOrRemoteNode(), false);
    }

    public void testReplicationTypesOverrideNotAllowed_IndexAPI() {
        // Generate mutually exclusive replication strategies at cluster and index level
        List<ReplicationType> replicationStrategies = getRandomReplicationTypesAsList();
        ReplicationType clusterLevelReplication = replicationStrategies.get(0);
        ReplicationType indexLevelReplication = replicationStrategies.get(1);
        Settings nodeSettings = Settings.builder()
            .put(CLUSTER_SETTING_REPLICATION_TYPE, clusterLevelReplication)
            .put(CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING.getKey(), true)
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        Settings indexSettings = Settings.builder().put(indexSettings()).put(SETTING_REPLICATION_TYPE, indexLevelReplication).build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> createIndex(INDEX_NAME, indexSettings));
        assertEquals(REPLICATION_MISMATCH_VALIDATION_ERROR, exception.getMessage());
    }

    public void testReplicationTypesOverrideNotAllowed_WithTemplates() {
        // Generate mutually exclusive replication strategies at cluster and index level
        List<ReplicationType> replicationStrategies = getRandomReplicationTypesAsList();
        ReplicationType clusterLevelReplication = replicationStrategies.get(0);
        ReplicationType templateReplicationType = replicationStrategies.get(1);
        Settings nodeSettings = Settings.builder()
            .put(CLUSTER_SETTING_REPLICATION_TYPE, clusterLevelReplication)
            .put(CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING.getKey(), true)
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        logger.info(
            "--> Create index with template replication {} and cluster level replication {}",
            templateReplicationType,
            clusterLevelReplication
        );
        // Create index template
        client().admin()
            .indices()
            .preparePutTemplate("template_1")
            .setPatterns(Collections.singletonList("test-idx*"))
            .setSettings(Settings.builder().put(indexSettings()).put(SETTING_REPLICATION_TYPE, templateReplicationType).build())
            .setOrder(0)
            .get();

        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates(), hasSize(1));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> createIndex(INDEX_NAME));
        assertEquals(REPLICATION_MISMATCH_VALIDATION_ERROR, exception.getMessage());
    }

    public void testReplicationTypesOverrideNotAllowed_WithResizeAction() {
        // Generate mutually exclusive replication strategies at cluster and index level
        List<ReplicationType> replicationStrategies = getRandomReplicationTypesAsList();
        ReplicationType clusterLevelReplication = replicationStrategies.get(0);
        ReplicationType indexLevelReplication = replicationStrategies.get(1);
        Settings nodeSettings = Settings.builder()
            .put(CLUSTER_SETTING_REPLICATION_TYPE, clusterLevelReplication)
            .put(CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING.getKey(), true)
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        logger.info(
            "--> Create index with index level replication {} and cluster level replication {}",
            indexLevelReplication,
            clusterLevelReplication
        );

        // Define resize action and target shard count.
        List<Tuple<ResizeType, Integer>> resizeActionsList = new ArrayList<>();
        final int initialShardCount = 2;
        resizeActionsList.add(new Tuple<>(ResizeType.SPLIT, 2 * initialShardCount));
        resizeActionsList.add(new Tuple<>(ResizeType.SHRINK, SHARD_COUNT));
        resizeActionsList.add(new Tuple<>(ResizeType.CLONE, initialShardCount));

        Tuple<ResizeType, Integer> resizeActionTuple = resizeActionsList.get(random().nextInt(resizeActionsList.size()));
        final String targetIndexName = resizeActionTuple.v1().name().toLowerCase(Locale.ROOT) + "-target";

        logger.info("--> Performing resize action {} with shard count {}", resizeActionTuple.v1(), resizeActionTuple.v2());

        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, initialShardCount)
            .put(SETTING_REPLICATION_TYPE, clusterLevelReplication)
            .build();
        createIndex(INDEX_NAME, indexSettings);

        // Block writes
        client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(Settings.builder().put("index.blocks.write", true)).get();
        ensureGreen();

        // Validate resize action fails
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareResizeIndex(INDEX_NAME, targetIndexName)
                .setResizeType(resizeActionTuple.v1())
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", resizeActionTuple.v2())
                        .putNull("index.blocks.write")
                        .put(SETTING_REPLICATION_TYPE, indexLevelReplication)
                        .build()
                )
                .get()
        );
        assertEquals(REPLICATION_MISMATCH_VALIDATION_ERROR, exception.getMessage());
    }

    /**
     * Generate a list of ReplicationType with random ordering
     *
     * @return List of ReplicationType values
     */
    private List<ReplicationType> getRandomReplicationTypesAsList() {
        List<ReplicationType> replicationStrategies = List.of(ReplicationType.SEGMENT, ReplicationType.DOCUMENT);
        int randomReplicationIndex = random().nextInt(replicationStrategies.size());
        ReplicationType clusterLevelReplication = replicationStrategies.get(randomReplicationIndex);
        ReplicationType indexLevelReplication = replicationStrategies.get(1 - randomReplicationIndex);
        return List.of(clusterLevelReplication, indexLevelReplication);
    }
}
