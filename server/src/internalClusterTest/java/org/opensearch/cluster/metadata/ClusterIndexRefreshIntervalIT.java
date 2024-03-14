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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class ClusterIndexRefreshIntervalIT extends AbstractSnapshotIntegTestCase {

    public static final String INDEX_NAME = "test-index";

    public static final String OTHER_INDEX_NAME = "other-test-index";

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().startClusterManagerOnlyNode();
    }

    static void putIndexTemplate(String refreshInterval) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("my-template"); // <1>
        request.patterns(Arrays.asList("pattern-1", "log-*")); // <2>

        request.settings(
            Settings.builder() // <1>
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put("index.refresh_interval", refreshInterval)
        );
        assertTrue(client().admin().indices().putTemplate(request).actionGet().isAcknowledged());
    }

    public void testIndexTemplateCreationSucceedsWhenNoMinimumRefreshInterval() throws ExecutionException, InterruptedException {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        putIndexTemplate("2s");

        // Test index creation using template with valid refresh interval
        String indexName = "log-myindex-1";
        createIndex(indexName);
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = indicesService.indexService(new Index(indexName, uuid));
        assertEquals(TimeValue.timeValueSeconds(2), indexService.getRefreshTaskInterval());
    }

    public void testDefaultRefreshIntervalWithUpdateClusterAndIndexSettings() throws Exception {
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        String uuid = getIndexResponse.getSettings().get(INDEX_NAME).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = indicesService.indexService(new Index(INDEX_NAME, uuid));
        assertEquals(getDefaultRefreshInterval(), indexService.getRefreshTaskInterval());

        // Update the cluster.default.index.refresh_interval setting to another value and validate the index refresh interval
        TimeValue refreshInterval = TimeValue.timeValueMillis(randomIntBetween(10, 90) * 1000L);
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval))
            .get();
        assertEquals(refreshInterval, indexService.getRefreshTaskInterval());

        // Update of cluster.minimum.index.refresh_interval setting to value more than default refreshInterval above will fail
        TimeValue invalidMinimumRefreshInterval = TimeValue.timeValueMillis(refreshInterval.millis() + randomIntBetween(1, 1000));
        IllegalArgumentException exceptionDuringMinUpdate = assertThrows(
            IllegalArgumentException.class,
            () -> client(clusterManagerName).admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), invalidMinimumRefreshInterval)
                )
                .get()
        );
        assertEquals(
            "cluster minimum index refresh interval ["
                + invalidMinimumRefreshInterval
                + "] more than cluster default index refresh interval ["
                + refreshInterval
                + "]",
            exceptionDuringMinUpdate.getMessage()
        );

        // Update the cluster.minimum.index.refresh_interval setting to a valid value, this will succeed.
        TimeValue validMinimumRefreshInterval = TimeValue.timeValueMillis(refreshInterval.millis() - randomIntBetween(1, 1000));
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), validMinimumRefreshInterval)
            )
            .get();

        // Update with invalid index setting index.refresh_interval, this will fail.
        TimeValue invalidRefreshInterval = TimeValue.timeValueMillis(validMinimumRefreshInterval.millis() - randomIntBetween(1, 1000));
        String expectedMessage = "invalid index.refresh_interval ["
            + invalidRefreshInterval
            + "]: cannot be smaller than cluster.minimum.index.refresh_interval ["
            + validMinimumRefreshInterval
            + "]";

        IllegalArgumentException exceptionDuringUpdateSettings = assertThrows(
            IllegalArgumentException.class,
            () -> client(clusterManagerName).admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(INDEX_NAME).settings(
                        Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), invalidRefreshInterval)
                    )
                )
                .actionGet()
        );
        assertEquals(expectedMessage, exceptionDuringUpdateSettings.getMessage());

        // Create another index with invalid index setting index.refresh_interval, this fails.
        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), invalidRefreshInterval)
            .build();
        IllegalArgumentException exceptionDuringCreateIndex = assertThrows(
            IllegalArgumentException.class,
            () -> createIndex(OTHER_INDEX_NAME, indexSettings)
        );
        assertEquals(expectedMessage, exceptionDuringCreateIndex.getMessage());

        // Update with valid index setting index.refresh_interval, this will succeed now.
        TimeValue validRefreshInterval = TimeValue.timeValueMillis(validMinimumRefreshInterval.millis() + randomIntBetween(1, 1000));
        client(clusterManagerName).admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(INDEX_NAME).settings(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), validRefreshInterval)
                )
            )
            .get();
        // verify refresh task interval is updated.
        assertEquals(validRefreshInterval, indexService.getRefreshTaskInterval());

        // Try to create another index with valid index setting index.refresh_interval, this will pass.
        createIndex(
            OTHER_INDEX_NAME,
            Settings.builder().put(indexSettings).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), validRefreshInterval).build()
        );
        getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        String otherUuid = getIndexResponse.getSettings().get(INDEX_NAME).get(IndexMetadata.SETTING_INDEX_UUID);
        assertEquals(validRefreshInterval, indicesService.indexService(new Index(OTHER_INDEX_NAME, otherUuid)).getRefreshTaskInterval());

        // Update the cluster.default.index.refresh_interval & cluster.minimum.index.refresh_interval setting to null
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .putNull(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey())
                    .putNull(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey())
            )
            .get();
        // verify the index is still using the refresh interval passed in the update settings call
        assertEquals(validRefreshInterval, indexService.getRefreshTaskInterval());

        // Remove the index setting as well now, it should reset the refresh task interval to the default refresh interval
        client(clusterManagerName).admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(INDEX_NAME).settings(
                    Settings.builder().putNull(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey())
                )
            )
            .get();
        assertEquals(getDefaultRefreshInterval(), indexService.getRefreshTaskInterval());
    }

    public void testRefreshIntervalDisabled() throws ExecutionException, InterruptedException {
        TimeValue clusterMinimumRefreshInterval = client().settings()
            .getAsTime(IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE);
        boolean createIndexSuccess = clusterMinimumRefreshInterval.equals(TimeValue.MINUS_ONE);
        String clusterManagerName = internalCluster().getClusterManagerName();
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), IndexSettings.MINIMUM_REFRESH_INTERVAL)
            .build();
        if (createIndexSuccess) {
            createIndex(INDEX_NAME, settings);
            ensureYellowAndNoInitializingShards(INDEX_NAME);
            ensureGreen(INDEX_NAME);
            GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
            String uuid = getIndexResponse.getSettings().get(INDEX_NAME).get(IndexMetadata.SETTING_INDEX_UUID);
            IndexService indexService = indicesService.indexService(new Index(INDEX_NAME, uuid));
            assertEquals(IndexSettings.MINIMUM_REFRESH_INTERVAL, indexService.getRefreshTaskInterval());
        } else {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> createIndex(INDEX_NAME, settings));
            assertEquals(
                "invalid index.refresh_interval [-1]: cannot be smaller than cluster.minimum.index.refresh_interval ["
                    + getMinRefreshIntervalForRefreshDisabled()
                    + "]",
                exception.getMessage()
            );
        }
    }

    protected TimeValue getMinRefreshIntervalForRefreshDisabled() {
        throw new RuntimeException("This is not expected to be called here, but for the implementor");
    }

    public void testInvalidRefreshInterval() {
        String invalidRefreshInterval = "-10s";
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), invalidRefreshInterval)
            .build();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> createIndex(INDEX_NAME, settings));
        assertEquals(
            "failed to parse setting [index.refresh_interval] with value ["
                + invalidRefreshInterval
                + "] as a time value: negative durations are not supported",
            exception.getMessage()
        );
    }

    public void testCreateIndexWithExplicitNullRefreshInterval() throws ExecutionException, InterruptedException {
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .putNull(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey())
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        GetIndexResponse getIndexResponse = client(internalCluster().getClusterManagerName()).admin()
            .indices()
            .getIndex(new GetIndexRequest())
            .get();
        String uuid = getIndexResponse.getSettings().get(INDEX_NAME).get(IndexMetadata.SETTING_INDEX_UUID);

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        IndexService indexService = indicesService.indexService(new Index(INDEX_NAME, uuid));

        assertEquals(IndexSettings.DEFAULT_REFRESH_INTERVAL, indexService.getRefreshTaskInterval());
    }

    /**
     * In this test we check the case where an index is created with index setting `index.refresh_interval` with the value
     * being lesser than the `cluster.minimum.index.refresh_interval`. Later we change the cluster minimum to be more than
     * the index setting. The underlying index should continue to use the same refresh interval as earlier.
     */
    public void testClusterMinimumChangeOnIndexWithCustomRefreshInterval() throws ExecutionException, InterruptedException {
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        TimeValue customRefreshInterval = TimeValue.timeValueSeconds(getDefaultRefreshInterval().getSeconds() + randomIntBetween(1, 5));
        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), customRefreshInterval)
            .build();
        createIndex(INDEX_NAME, indexSettings);

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        GetIndexResponse getIndexResponse = client(internalCluster().getClusterManagerName()).admin()
            .indices()
            .getIndex(new GetIndexRequest())
            .get();
        String uuid = getIndexResponse.getSettings().get(INDEX_NAME).get(IndexMetadata.SETTING_INDEX_UUID);

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, randomFrom(dataNodes));
        IndexService indexService = indicesService.indexService(new Index(INDEX_NAME, uuid));

        assertEquals(customRefreshInterval, indexService.getRefreshTaskInterval());

        // Update the cluster.minimum.index.refresh_interval setting to a valid value higher the custom refresh interval.
        // At the same time, due to certain degree of randomness in the test, we update the cluster.default.refresh_interval
        // to a valid value as well to be deterministic in test behaviour.
        TimeValue clusterMinimum = TimeValue.timeValueSeconds(customRefreshInterval.getSeconds() + randomIntBetween(1, 5));
        TimeValue clusterDefault = TimeValue.timeValueSeconds(customRefreshInterval.getSeconds() + 6);
        String clusterManagerName = internalCluster().getClusterManagerName();
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), clusterDefault)
                    .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), clusterMinimum)
            )
            .get();

        // Validate that the index refresh interval is still the existing one that was used during index creation
        assertEquals(customRefreshInterval, indexService.getRefreshTaskInterval());

        // Update index setting to a value >= current cluster minimum and this should happen successfully.
        customRefreshInterval = TimeValue.timeValueSeconds(clusterMinimum.getSeconds() + randomIntBetween(1, 5));
        client(clusterManagerName).admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(INDEX_NAME).settings(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), customRefreshInterval)
                )
            )
            .get();
        assertEquals(customRefreshInterval, indexService.getRefreshTaskInterval());
    }

    protected TimeValue getDefaultRefreshInterval() {
        return IndexSettings.DEFAULT_REFRESH_INTERVAL;
    }
}
