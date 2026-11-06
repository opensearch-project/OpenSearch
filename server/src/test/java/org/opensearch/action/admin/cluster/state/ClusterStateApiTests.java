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

package org.opensearch.action.admin.cluster.state;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsAction;
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.index.fielddomain.ClusterStateFieldDomainProvider;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateApiTests extends OpenSearchSingleNodeTestCase {

    public void testForwardedResponseFiltersPrivateMetadata() throws IOException {
        Index index = createIndex("logs-forwarded").index();
        ClusterState original = getInstanceFromNode(ClusterService.class).state();
        Map<String, String> privateData = Map.of("field", "private-value");
        Map<String, String> publicData = Map.of("key", "public-value");
        IndexMetadata metadata = IndexMetadata.builder(original.metadata().index(index))
            .putCustom(IndexFieldDomainMetadata.CUSTOM_KEY, privateData)
            .putCustom("plugin_metadata", publicData)
            .build();
        ClusterState unfiltered = ClusterState.builder(original)
            .metadata(Metadata.builder(original.metadata()).put(metadata, false))
            .build();
        // An unpatched cluster manager can return unfiltered state when the local node forwards the request.
        ClusterStateResponse response = readForwardedResponse(new ClusterStateResponse(unfiltered.getClusterName(), unfiltered, false));
        assertFalse(response.isWaitForTimedOut());
        assertEquals(unfiltered.getClusterName(), response.getClusterName());
        assertEquals(unfiltered.version(), response.getState().version());
        assertEquals(unfiltered.stateUUID(), response.getState().stateUUID());
        assertEquals(unfiltered.metadata().version(), response.getState().metadata().version());
        assertEquals(metadata.getVersion(), response.getState().metadata().index(index).getVersion());
        assertNull(response.getState().metadata().index(index).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
        assertEquals(publicData, response.getState().metadata().index(index).getCustomData("plugin_metadata"));
        assertEquals(privateData, unfiltered.metadata().index(index).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
        // Check what the API sends next without relying on filtering in the receiving response class.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                ClusterStateResponse transported = new ClusterStateResponse(in);
                assertNull(transported.getState().metadata().index(index).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                assertEquals(publicData, transported.getState().metadata().index(index).getCustomData("plugin_metadata"));
            }
        }
    }

    public void testForwardedResponsePreservesTimeoutAndPublicState() throws IOException {
        ClusterState state = getInstanceFromNode(ClusterService.class).state();
        ClusterStateResponse timedOut = readForwardedResponse(new ClusterStateResponse(state.getClusterName(), null, true));
        assertTrue(timedOut.isWaitForTimedOut());
        assertNull(timedOut.getState());
        assertEquals(state.getClusterName(), timedOut.getClusterName());
        ClusterStateResponse response = readForwardedResponse(new ClusterStateResponse(state.getClusterName(), state, false));
        assertEquals(state.metadata().indices(), response.getState().metadata().indices());
        assertEquals(state.metadata().customs(), response.getState().metadata().customs());
        assertEquals(state.metadata().version(), response.getState().metadata().version());
        assertEquals(state.version(), response.getState().version());
        assertFalse(response.isWaitForTimedOut());
    }

    private ClusterStateResponse readForwardedResponse(ClusterStateResponse response) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                return getInstanceFromNode(TransportClusterStateAction.class).read(in);
            }
        }
    }

    public void testFieldDomainsAreExcludedFromClusterStateResponses() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        DateRangeFieldDomain domain = new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer");
        Map<String, String> customData = IndexFieldDomainMetadata.getInstance().toCustomData(domain);
        for (String indexName : List.of("logs-000001", "logs-000002")) {
            Index index = createIndex(indexName).index();
            assertAcked(
                client().execute(PutIndexFieldDomainsAction.INSTANCE, new PutIndexFieldDomainsRequest(index).fieldDomain(domain))
                    .actionGet()
            );
        }
        createIndex("other");
        Map<String, String> publicCustomData = Map.of("key", "value");
        PlainActionFuture<Void> updated = new PlainActionFuture<>();
        clusterService.submitStateUpdateTask("add-public-index-custom-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata.Builder metadata = Metadata.builder(currentState.metadata());
                for (IndexMetadata indexMetadata : currentState.metadata()) {
                    metadata.put(IndexMetadata.builder(indexMetadata).putCustom("plugin_metadata", publicCustomData));
                }
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                updated.onResponse(null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                updated.onFailure(e);
            }
        });
        updated.actionGet();
        assertFalse(client().admin().cluster().prepareHealth().setWaitForGreenStatus().get().isTimedOut());

        for (boolean local : List.of(false, true)) {
            for (String[] indices : List.of(
                new String[0],
                new String[] { "logs-000001" },
                new String[] { "logs-*" },
                new String[] { "_all" }
            )) {
                ClusterState stateBeforeRequest = clusterService.state();
                ClusterStateResponse response = client().admin()
                    .cluster()
                    .prepareState()
                    .clear()
                    .setMetadata(true)
                    .setLocal(local)
                    .setIndices(indices)
                    .get();
                assertFalse(response.getState().metadata().indices().isEmpty());
                Set<String> expectedIndices = indices.length == 1 && indices[0].equals("logs-000001") ? Set.of("logs-000001")
                    : indices.length == 1 && indices[0].equals("logs-*") ? Set.of("logs-000001", "logs-000002")
                    : stateBeforeRequest.metadata().indices().keySet();
                assertEquals(expectedIndices, response.getState().metadata().indices().keySet());
                for (IndexMetadata indexMetadata : response.getState().metadata()) {
                    assertNull(indexMetadata.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                    assertEquals(publicCustomData, indexMetadata.getCustomData("plugin_metadata"));
                    IndexMetadata expected = stateBeforeRequest.metadata().index(indexMetadata.getIndex());
                    IndexMetadata.Builder expectedResponse = IndexMetadata.builder(expected);
                    expectedResponse.removeCustom(IndexFieldDomainMetadata.CUSTOM_KEY);
                    assertEquals(expectedResponse.build(), indexMetadata);
                }
                for (String indexName : List.of("logs-000001", "logs-000002")) {
                    assertEquals(
                        customData,
                        stateBeforeRequest.metadata().index(indexName).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY)
                    );
                    assertEquals(
                        customData,
                        clusterService.state().metadata().index(indexName).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY)
                    );
                    assertEquals(
                        customData,
                        IndexFieldDomainMetadata.getInstance()
                            .toCustomData(
                                new ClusterStateFieldDomainProvider().getDomain(clusterService.state(), indexName, "@timestamp")
                                    .orElseThrow()
                            )
                    );
                }
            }
        }
    }

    public void testPrivateMetadataWithAliasesHiddenAndClosedIndices() {
        DateRangeFieldDomain domain = new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer");
        for (String indexName : List.of("logs-open", "logs-hidden", "logs-closed")) {
            Index index = createIndex(indexName, Settings.builder().put("index.hidden", indexName.equals("logs-hidden")).build()).index();
            assertAcked(
                client().execute(PutIndexFieldDomainsAction.INSTANCE, new PutIndexFieldDomainsRequest(index).fieldDomain(domain))
                    .actionGet()
            );
        }
        assertAcked(client().admin().indices().prepareAliases().addAlias("logs-open", "logs-alias").get());
        assertAcked(client().admin().indices().prepareClose("logs-closed").get());
        Map<String, Set<String>> selections = Map.of(
            "logs-alias",
            Set.of("logs-open"),
            "logs-hidden",
            Set.of("logs-hidden"),
            "logs-closed",
            Set.of("logs-closed"),
            "logs-*",
            Set.of("logs-open", "logs-hidden", "logs-closed")
        );
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        for (boolean local : List.of(false, true)) {
            for (Map.Entry<String, Set<String>> selection : selections.entrySet()) {
                ClusterStateResponse response = client().admin()
                    .cluster()
                    .prepareState()
                    .clear()
                    .setMetadata(true)
                    .setLocal(local)
                    .setIndices(selection.getKey())
                    .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                    .get();
                assertEquals(selection.getValue(), response.getState().metadata().indices().keySet());
                for (String indexName : selection.getValue()) {
                    assertNull(response.getState().metadata().index(indexName).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                    assertEquals(
                        IndexFieldDomainMetadata.getInstance().toCustomData(domain),
                        clusterService.state().metadata().index(indexName).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY)
                    );
                }
            }
            ClusterStateResponse withoutMetadata = client().admin().cluster().prepareState().clear().setNodes(true).setLocal(local).get();
            assertTrue(withoutMetadata.getState().metadata().indices().isEmpty());
        }
    }

    public void testWaitForMetadataVersion() throws Exception {
        Index index = createIndex("logs-wait").index();
        DateRangeFieldDomain domain = new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer");
        assertAcked(
            client().execute(PutIndexFieldDomainsAction.INSTANCE, new PutIndexFieldDomainsRequest(index).fieldDomain(domain)).actionGet()
        );
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForTimeout(TimeValue.timeValueHours(1));
        ActionFuture<ClusterStateResponse> future1 = client().admin().cluster().state(clusterStateRequest);
        assertThat(future1.isDone(), is(true));
        assertThat(future1.actionGet().isWaitForTimedOut(), is(false));
        assertNull(future1.actionGet().getState().metadata().index(index).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
        long metadataVersion = future1.actionGet().getState().getMetadata().version();

        // Verify that cluster state api returns after the cluster settings have been updated:
        clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);

        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(clusterStateRequest);
        assertThat(future2.isDone(), is(false));

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        // Pick an arbitrary dynamic cluster setting and change it. Just to get metadata version incremented:
        updateSettingsRequest.transientSettings(Settings.builder().put("cluster.max_shards_per_node", 999));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        assertBusy(() -> { assertThat(future2.isDone(), is(true)); });
        ClusterStateResponse response = future2.actionGet();
        assertThat(response.isWaitForTimedOut(), is(false));
        assertThat(response.getState().metadata().version(), equalTo(metadataVersion + 1));
        assertNull(response.getState().metadata().index(index).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
        assertEquals(
            IndexFieldDomainMetadata.getInstance().toCustomData(domain),
            getInstanceFromNode(ClusterService.class).state().metadata().index(index).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY)
        );

        // Verify that the timed out property has been set"
        metadataVersion = response.getState().getMetadata().version();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);
        clusterStateRequest.waitForTimeout(TimeValue.timeValueMillis(500)); // Fail fast
        ActionFuture<ClusterStateResponse> future3 = client().admin().cluster().state(clusterStateRequest);
        assertBusy(() -> { assertThat(future3.isDone(), is(true)); });
        response = future3.actionGet();
        assertThat(response.isWaitForTimedOut(), is(true));
        assertThat(response.getState(), nullValue());

        // Remove transient setting, otherwise test fails with the reason that this test leaves state behind:
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(Settings.builder().put("cluster.max_shards_per_node", (String) null));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

}
