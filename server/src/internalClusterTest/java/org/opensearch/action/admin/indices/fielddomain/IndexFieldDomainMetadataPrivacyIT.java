/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddomain.ClusterStateFieldDomainProvider;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class IndexFieldDomainMetadataPrivacyIT extends OpenSearchIntegTestCase {
    public void testFieldDomainsSurviveFullRestart() throws Exception {
        Map<String, String> domains = createIndexWithDomains("logs-original");
        assertDomainsAreInternal("logs-original", domains);
        internalCluster().fullRestart();
        ensureGreen("logs-original");
        assertDomainsAreInternal("logs-original", domains);
    }

    public void testFieldDomainsSurviveSnapshotRestore() throws Exception {
        Map<String, String> domains = createIndexWithDomains("logs-original");
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository("repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
                .get()
        );
        assertEquals(
            SnapshotState.SUCCESS,
            client().admin()
                .cluster()
                .prepareCreateSnapshot("repo", "snapshot")
                .setIndices("logs-original")
                .setIncludeGlobalState(false)
                .setWaitForCompletion(true)
                .get()
                .getSnapshotInfo()
                .state()
        );
        assertAcked(client().admin().indices().prepareDelete("logs-original").get());
        assertEquals(
            1,
            client().admin()
                .cluster()
                .prepareRestoreSnapshot("repo", "snapshot")
                .setIndices("logs-original")
                .setRenamePattern("logs-original")
                .setRenameReplacement("logs-restored")
                .setRestoreGlobalState(false)
                .setWaitForCompletion(true)
                .get()
                .getRestoreInfo()
                .successfulShards()
        );
        ensureGreen("logs-restored");
        assertDomainsAreInternal("logs-restored", domains);
    }

    private Map<String, String> createIndexWithDomains(String indexName) {
        assertAcked(
            prepareCreate(indexName).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("@timestamp", "type=date")
        );
        ensureGreen(indexName);
        client().prepareIndex(indexName).setId("1").setSource("@timestamp", 100L).get();
        refresh(indexName);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put("index.blocks.write", true))
                .get()
        );
        DateRangeFieldDomain domain = new DateRangeFieldDomain("@timestamp", 100L, 100L, true, "test_producer");
        var index = internalCluster().clusterService(internalCluster().getClusterManagerName())
            .state()
            .metadata()
            .index(indexName)
            .getIndex();
        assertAcked(
            client().execute(PutIndexFieldDomainsAction.INSTANCE, new PutIndexFieldDomainsRequest(index).fieldDomain(domain)).actionGet()
        );
        return IndexFieldDomainMetadata.getInstance().toCustomData(domain);
    }

    private void assertDomainsAreInternal(String indexName, Map<String, String> domains) throws Exception {
        assertBusy(() -> {
            for (ClusterService service : internalCluster().getInstances(ClusterService.class)) {
                assertEquals(domains, service.state().metadata().index(indexName).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                assertEquals(
                    domains,
                    IndexFieldDomainMetadata.getInstance()
                        .toCustomData(
                            new ClusterStateFieldDomainProvider().getDomain(service.state(), indexName, "@timestamp").orElseThrow()
                        )
                );
            }
        });
        for (String node : internalCluster().getNodeNames()) {
            for (boolean local : List.of(false, true)) {
                assertNull(
                    client(node).admin()
                        .cluster()
                        .prepareState()
                        .clear()
                        .setMetadata(true)
                        .setIndices(indexName)
                        .setLocal(local)
                        .get()
                        .getState()
                        .metadata()
                        .index(indexName)
                        .getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY)
                );
            }
        }
        assertHitCount(client().prepareSearch(indexName).get(), 1L);
    }
}
