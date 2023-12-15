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
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.indices.IndicesService.CLUSTER_FORCE_INDEX_REPLICATION_TYPE_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_SETTING_REPLICATION_TYPE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.hasSize;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationClusterSettingIT extends OpenSearchIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    private static final String SYSTEM_INDEX_NAME = ".test-system-index";
    protected static final int SHARD_COUNT = 1;
    protected static final int REPLICA_COUNT = 1;

    protected static final String REPLICATION_MISMATCH_VALIDATION_ERROR =
        "Validation Failed: 1: index setting [index.replication.type] is not allowed to be set as [cluster.force.index.replication_type=true];";

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
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
        assertEquals(indicesService.indexService(index).getIndexSettings().isSegRepEnabled(), false);
        assertEquals(indicesService.indexService(anotherIndex).getIndexSettings().isSegRepEnabled(), true);
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
        assertEquals(indicesService.indexService(index).getIndexSettings().isSegRepEnabled(), true);
        assertEquals(indicesService.indexService(anotherIndex).getIndexSettings().isSegRepEnabled(), false);
    }

    public void testDifferentReplicationTypes_CreateIndex_StrictMode() throws Throwable {
        final int documentCount = scaledRandomIntBetween(1, 10);
        BiConsumer<List<ReplicationType>, List<String>> consumer = (replicationList, dataNodesList) -> {
            Settings indexSettings = Settings.builder().put(indexSettings()).put(SETTING_REPLICATION_TYPE, replicationList.get(1)).build();
            createIndex(INDEX_NAME, indexSettings);
        };
        executeTest(true, consumer, INDEX_NAME, documentCount);
    }

    public void testDifferentReplicationTypes_CreateIndex_NonStrictMode() throws Throwable {
        final int documentCount = scaledRandomIntBetween(1, 10);
        BiConsumer<List<ReplicationType>, List<String>> consumer = (replicationList, dataNodesList) -> {
            Settings indexSettings = Settings.builder().put(indexSettings()).put(SETTING_REPLICATION_TYPE, replicationList.get(1)).build();
            createIndex(INDEX_NAME, indexSettings);
            for (int i = 0; i < documentCount; i++) {
                client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", "bar").get();
            }
        };
        executeTest(false, consumer, INDEX_NAME, documentCount);
    }

    public void testDifferentReplicationTypes_IndexTemplates_StrictMode() throws Throwable {
        final int documentCount = scaledRandomIntBetween(1, 10);

        BiConsumer<List<ReplicationType>, List<String>> consumer = (replicationList, dataNodesList) -> {
            client().admin()
                .indices()
                .preparePutTemplate("template_1")
                .setPatterns(Collections.singletonList("test-idx*"))
                .setSettings(Settings.builder().put(indexSettings()).put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build())
                .setOrder(0)
                .get();

            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
            assertThat(response.getIndexTemplates(), hasSize(1));

            createIndex(INDEX_NAME);
        };
        executeTest(true, consumer, INDEX_NAME, documentCount);
    }

    public void testDifferentReplicationTypes_IndexTemplates_NonStrictMode() throws Throwable {
        final int documentCount = scaledRandomIntBetween(1, 10);

        // Create an index using current cluster level settings
        BiConsumer<List<ReplicationType>, List<String>> consumer = (replicationList, dataNodesList) -> {
            client().admin()
                .indices()
                .preparePutTemplate("template_1")
                .setPatterns(Collections.singletonList("test-idx*"))
                .setSettings(Settings.builder().put(indexSettings()).put(SETTING_REPLICATION_TYPE, replicationList.get(1)).build())
                .setOrder(0)
                .get();

            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
            assertThat(response.getIndexTemplates(), hasSize(1));

            createIndex(INDEX_NAME);
            for (int i = 0; i < documentCount; i++) {
                client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", "bar").get();
            }
        };
        executeTest(false, consumer, INDEX_NAME, documentCount);
    }

    public void testMismatchingReplicationType_ResizeAction_StrictMode() throws Throwable {
        // Define resize action and target shard count.
        List<Tuple<ResizeType, Integer>> resizeActionsList = new ArrayList<>();
        final int initialShardCount = 2;
        resizeActionsList.add(new Tuple<>(ResizeType.SPLIT, 2 * initialShardCount));
        resizeActionsList.add(new Tuple<>(ResizeType.SHRINK, SHARD_COUNT));
        resizeActionsList.add(new Tuple<>(ResizeType.CLONE, initialShardCount));

        Tuple<ResizeType, Integer> resizeActionTuple = resizeActionsList.get(random().nextInt(resizeActionsList.size()));
        final String targetIndexName = resizeActionTuple.v1().name().toLowerCase(Locale.ROOT) + "-target";
        final int documentCount = scaledRandomIntBetween(1, 10);

        logger.info("--> Performing resize action {} with shard count {}", resizeActionTuple.v1(), resizeActionTuple.v2());

        // Create an index using current cluster level settings
        BiConsumer<List<ReplicationType>, List<String>> consumer = (replicationList, dataNodesList) -> {
            Settings indexSettings = Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, initialShardCount)
                .put(SETTING_REPLICATION_TYPE, replicationList.get(0))
                .build();
            createIndex(INDEX_NAME, indexSettings);

            for (int i = 0; i < documentCount; i++) {
                client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", "bar").get();
            }

            // Block writes
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put("index.blocks.write", true))
                .get();
            ensureGreen();

            try {
                for (String node : dataNodesList) {
                    assertBusy(() -> {
                        assertHitCount(
                            client(node).prepareSearch(INDEX_NAME).setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
                            documentCount
                        );
                    }, 30, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // Test create index fails
            client().admin()
                .indices()
                .prepareResizeIndex(INDEX_NAME, targetIndexName)
                .setResizeType(resizeActionTuple.v1())
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", resizeActionTuple.v2())
                        .putNull("index.blocks.write")
                        .put(SETTING_REPLICATION_TYPE, replicationList.get(1))
                        .build()
                )
                .get();
        };
        executeTest(true, consumer, targetIndexName, documentCount);
    }

    public void testMismatchingReplicationType_ResizeAction_NonStrictMode() throws Throwable {
        final int initialShardCount = 2;
        // Define resize action and target shard count.
        List<Tuple<ResizeType, Integer>> resizeActionsList = new ArrayList<>();
        resizeActionsList.add(new Tuple<>(ResizeType.SPLIT, 2 * initialShardCount));
        resizeActionsList.add(new Tuple<>(ResizeType.SHRINK, SHARD_COUNT));
        resizeActionsList.add(new Tuple<>(ResizeType.CLONE, initialShardCount));

        Tuple<ResizeType, Integer> resizeActionTuple = resizeActionsList.get(random().nextInt(resizeActionsList.size()));
        final String targetIndexName = resizeActionTuple.v1().name().toLowerCase(Locale.ROOT) + "-target";
        final int documentCount = scaledRandomIntBetween(1, 10);

        logger.info("--> Performing resize action {} with shard count {}", resizeActionTuple.v1(), resizeActionTuple.v2());

        // Create an index using current cluster level settings
        BiConsumer<List<ReplicationType>, List<String>> consumer = (replicationList, dataNodesList) -> {
            Settings indexSettings = Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, initialShardCount)
                .put(SETTING_REPLICATION_TYPE, replicationList.get(0))
                .build();

            createIndex(INDEX_NAME, indexSettings);

            for (int i = 0; i < documentCount; i++) {
                client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", "bar").get();
            }

            // Block writes
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put("index.blocks.write", true))
                .get();
            ensureGreen();

            try {
                for (String node : dataNodesList) {
                    assertBusy(() -> {
                        assertHitCount(
                            client(node).prepareSearch(INDEX_NAME).setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
                            documentCount
                        );
                    }, 30, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            client().admin()
                .indices()
                .prepareResizeIndex(INDEX_NAME, targetIndexName)
                .setResizeType(resizeActionTuple.v1())
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", resizeActionTuple.v2())
                        .putNull("index.blocks.write")
                        .put(SETTING_REPLICATION_TYPE, replicationList.get(1))
                        .build()
                )
                .get();
        };
        executeTest(false, consumer, targetIndexName, documentCount);
    }

    // Creates a cluster with mis-matching cluster level and index level replication strategies and validates that index
    // creation fails when cluster level setting `cluster.force.index.replication_type` is set to true and creation goes
    // through when it is false.
    private void executeTest(
        boolean restrictIndexLevelReplicationTypeSetting,
        BiConsumer<List<ReplicationType>, List<String>> createIndexRunnable,
        String targetIndexName,
        int documentCount
    ) throws Throwable {
        // Generate mutually exclusive replication strategies at cluster and index level
        List<ReplicationType> replicationStrategies = getRandomReplicationTypesAsList();
        ReplicationType clusterLevelReplication = replicationStrategies.get(0);
        ReplicationType indexLevelReplication = replicationStrategies.get(1);

        Settings settings = Settings.builder()
            .put(CLUSTER_SETTING_REPLICATION_TYPE, clusterLevelReplication)
            .put(CLUSTER_FORCE_INDEX_REPLICATION_TYPE_SETTING.getKey(), restrictIndexLevelReplicationTypeSetting)
            .build();
        internalCluster().startClusterManagerOnlyNode(settings);
        final String dataNodeOne = internalCluster().startDataOnlyNode(settings);
        final String dataNodeTwo = internalCluster().startDataOnlyNode(settings);
        List<String> dataNodes = Arrays.asList(dataNodeOne, dataNodeTwo);

        logger.info(
            "--> Create index with cluster level setting {} and index level replication setting {}",
            clusterLevelReplication,
            indexLevelReplication
        );

        if (restrictIndexLevelReplicationTypeSetting) {
            try {
                createIndexRunnable.accept(replicationStrategies, dataNodes);
            } catch (IllegalArgumentException exception) {
                assertEquals(REPLICATION_MISMATCH_VALIDATION_ERROR, exception.getMessage());
            }
        } else {
            createIndexRunnable.accept(replicationStrategies, dataNodes);
            ensureGreen(targetIndexName);
            for (String node : dataNodes) {
                assertBusy(() -> {
                    assertHitCount(
                        client(node).prepareSearch(targetIndexName).setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
                        documentCount
                    );
                });
            }
        }
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
