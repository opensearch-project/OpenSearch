/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.IndicesService;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;

import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertIndexTemplateExists;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertIndexTemplateMissing;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ClusterIndexRefreshIntervalWithNodeSettingsIT extends ClusterIndexRefreshIntervalIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), getDefaultRefreshInterval())
            .put(
                IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(),
                getMinRefreshIntervalForRefreshDisabled().toString()
            )
            .build();
    }

    public void testIndexTemplateCreationFailsWithLessThanMinimumRefreshInterval() throws ExecutionException, InterruptedException {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> putIndexTemplate("0s"));
        assertEquals(
            throwable.getMessage(),
            String.format(
                Locale.ROOT,
                "invalid index.refresh_interval [%s]: cannot be smaller than cluster.minimum.index.refresh_interval [%s]",
                "0s",
                getMinRefreshIntervalForRefreshDisabled()
            )
        );
    }

    public void testIndexTemplateSnapshotRestoreWithLessThanMinimumRefreshInterval() throws ExecutionException, InterruptedException {
        putIndexTemplate("2s");
        createRepository("test-repo", "fs");

        final SnapshotInfo snapshotInfo = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setIndices()
            .setWaitForCompletion(true)
            .execute()
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertThat(snapshotInfo.totalShards(), equalTo(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(0));

        assertThat(client().admin().indices().prepareDeleteTemplate("my-template").get().isAcknowledged(), equalTo(true));

        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "my-template");

        String clusterManagerName = internalCluster().getClusterManagerName();
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                    .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "4s")
            )
            .get();

        logger.info("--> try restore cluster state -- should fail");
        Throwable throwable = assertThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setRestoreGlobalState(true)
                .execute()
                .actionGet()
        );
        assertEquals(
            throwable.getMessage(),
            "invalid index.refresh_interval [2s]: cannot be smaller than cluster.minimum.index.refresh_interval [4s]"
        );

        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                    .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            )
            .get();

        logger.info("--> restore cluster state");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateExists(getIndexTemplatesResponse, "my-template");

    }

    public void testIndexSnapshotRestoreWithLessThanMinimumRefreshInterval() throws ExecutionException, InterruptedException {
        createIndex(
            "my-index",
            Settings.builder()
                .put(indexSettings())
                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), "2s")
                .build()
        );

        createRepository("test-repo", "fs");

        final SnapshotInfo snapshotInfo = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setIndices()
            .setWaitForCompletion(true)
            .execute()
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertThat(snapshotInfo.totalShards(), equalTo(1));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));

        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices("my-index")).get();
        assertEquals(1, getIndexResponse.indices().length);
        assertEquals("2s", getIndexResponse.getSetting("my-index", INDEX_REFRESH_INTERVAL_SETTING.getKey()));

        assertThat(client().admin().indices().prepareDelete("my-index").get().isAcknowledged(), equalTo(true));

        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest()).get();
        assertEquals(getIndexResponse.indices().length, 0);

        String clusterManagerName = internalCluster().getClusterManagerName();
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                    .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "4s")
            )
            .get();

        logger.info("--> try restore cluster state -- should fail");
        Throwable throwable = assertThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setRestoreGlobalState(true)
                .execute()
                .actionGet()
        );
        assertEquals(
            throwable.getMessage(),
            "invalid index.refresh_interval [2s]: cannot be smaller than cluster.minimum.index.refresh_interval [4s]"
        );

        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                    .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            )
            .get();

        logger.info("--> try restore cluster state -- should pass");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(1));

        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices("my-index")).get();
        assertEquals(getIndexResponse.indices().length, 1);
    }

    @Override
    protected TimeValue getMinRefreshIntervalForRefreshDisabled() {
        return TimeValue.timeValueSeconds(1);
    }

    @Override
    protected TimeValue getDefaultRefreshInterval() {
        return TimeValue.timeValueSeconds(5);
    }
}
