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

package org.opensearch.action.bulk;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.VersionType;
import org.opensearch.indices.SystemIndices;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportBulkActionIndicesThatCannotBeCreatedTests extends OpenSearchTestCase {
    public void testNonExceptional() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(randomAlphaOfLength(5)));
        bulkRequest.add(new IndexRequest(randomAlphaOfLength(5)));
        bulkRequest.add(new DeleteRequest(randomAlphaOfLength(5)));
        bulkRequest.add(new UpdateRequest(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5)));
        // Test emulating auto_create_index=false
        indicesThatCannotBeCreatedTestCase(emptySet(), bulkRequest, null);
        // Test emulating auto_create_index=true
        indicesThatCannotBeCreatedTestCase(emptySet(), bulkRequest, index -> true);
        // Test emulating all indices already created
        indicesThatCannotBeCreatedTestCase(emptySet(), bulkRequest, index -> false);
        // Test emulating auto_create_index=true with some indices already created.
        indicesThatCannotBeCreatedTestCase(emptySet(), bulkRequest, index -> randomBoolean());
    }

    public void testAllFail() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("no"));
        bulkRequest.add(new IndexRequest("can't"));
        bulkRequest.add(new DeleteRequest("do").version(0).versionType(VersionType.EXTERNAL));
        bulkRequest.add(new UpdateRequest("nothin", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        indicesThatCannotBeCreatedTestCase(new HashSet<>(Arrays.asList("no", "can't", "do", "nothin")), bulkRequest, index -> {
            throw new IndexNotFoundException("Can't make it because I say so");
        });
    }

    public void testSomeFail() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("ok"));
        bulkRequest.add(new IndexRequest("bad"));
        // Emulate auto_create_index=-bad,+*
        indicesThatCannotBeCreatedTestCase(singleton("bad"), bulkRequest, index -> {
            if (index.equals("bad")) {
                throw new IndexNotFoundException("Can't make it because I say so");
            }
            return true;
        });
        // Emulate auto_create_index=false but the "ok" index already exists
        indicesThatCannotBeCreatedTestCase(singleton("bad"), bulkRequest, index -> {
            if (index.equals("bad")) {
                throw new IndexNotFoundException("Can't make it because I say so");
            }
            return false;
        });
    }


    private void indicesThatCannotBeCreatedTestCase(Set<String> expected,
            BulkRequest bulkRequest, Function<String, Boolean> shouldAutoCreate) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(state.metadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterService.state()).thenReturn(state);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(state.getNodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.isIngestNode()).thenReturn(randomBoolean());
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService direct = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(direct);
        TransportBulkAction action = new TransportBulkAction(threadPool, mock(TransportService.class), clusterService,
            null, null, null, mock(ActionFilters.class), null, null,
            new IndexingPressureService(Settings.EMPTY, new ClusterService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null)), new SystemIndices(emptyMap())) {
            @Override
            void executeBulk(Task task, BulkRequest bulkRequest, long startTimeNanos, ActionListener<BulkResponse> listener,
                             AtomicArray<BulkItemResponse> responses, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
                assertEquals(expected, indicesThatCannotBeCreated.keySet());
            }

            @Override
            boolean needToCheck() {
                return null != shouldAutoCreate; // Use "null" to mean "no indices can be created so don't bother checking"
            }

            @Override
            boolean shouldAutoCreate(String index, ClusterState state) {
                return shouldAutoCreate.apply(index);
            }

            @Override
            void createIndex(String index, TimeValue timeout, Version minNodeVersion, ActionListener<CreateIndexResponse> listener) {
                // If we try to create an index just immediately assume it worked
                listener.onResponse(new CreateIndexResponse(true, true, index) {});
            }
        };
        action.doExecute(null, bulkRequest, null);
    }
}
