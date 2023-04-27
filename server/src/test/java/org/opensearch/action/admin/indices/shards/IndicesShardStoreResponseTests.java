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

package org.opensearch.action.admin.indices.shards;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.ImmutableOpenIntMap;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.NodeDisconnectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class IndicesShardStoreResponseTests extends OpenSearchTestCase {
    public void testBasicSerialization() throws Exception {
        final Map<String, ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>>> indexStoreStatuses = new HashMap<>();

        List<IndicesShardStoresResponse.Failure> failures = new ArrayList<>();
        ImmutableOpenIntMap.Builder<List<IndicesShardStoresResponse.StoreStatus>> storeStatuses = ImmutableOpenIntMap.builder();

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        List<IndicesShardStoresResponse.StoreStatus> storeStatusList = new ArrayList<>();
        storeStatusList.add(
            new IndicesShardStoresResponse.StoreStatus(node1, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null)
        );
        storeStatusList.add(
            new IndicesShardStoresResponse.StoreStatus(
                node2,
                UUIDs.randomBase64UUID(),
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA,
                null
            )
        );
        storeStatusList.add(
            new IndicesShardStoresResponse.StoreStatus(
                node1,
                UUIDs.randomBase64UUID(),
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED,
                new IOException("corrupted")
            )
        );
        storeStatuses.put(0, storeStatusList);
        storeStatuses.put(1, storeStatusList);
        ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> storesMap = storeStatuses.build();
        indexStoreStatuses.put("test", storesMap);
        indexStoreStatuses.put("test2", storesMap);

        failures.add(new IndicesShardStoresResponse.Failure("node1", "test", 3, new NodeDisconnectedException(node1, "")));

        IndicesShardStoresResponse storesResponse = new IndicesShardStoresResponse(
            indexStoreStatuses,
            Collections.unmodifiableList(failures)
        );
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        storesResponse.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        BytesReference bytes = BytesReference.bytes(contentBuilder);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
            Map<String, Object> map = parser.map();
            List<?> failureList = (List<?>) map.get("failures");
            assertThat(failureList.size(), equalTo(1));
            @SuppressWarnings("unchecked")
            Map<String, ?> failureMap = (Map<String, ?>) failureList.get(0);
            assertThat(failureMap.containsKey("index"), equalTo(true));
            assertThat(((String) failureMap.get("index")), equalTo("test"));
            assertThat(failureMap.containsKey("shard"), equalTo(true));
            assertThat(((int) failureMap.get("shard")), equalTo(3));
            assertThat(failureMap.containsKey("node"), equalTo(true));
            assertThat(((String) failureMap.get("node")), equalTo("node1"));

            @SuppressWarnings("unchecked")
            Map<String, Object> indices = (Map<String, Object>) map.get("indices");
            for (String index : new String[] { "test", "test2" }) {
                assertThat(indices.containsKey(index), equalTo(true));
                @SuppressWarnings("unchecked")
                Map<String, Object> shards = ((Map<String, Object>) ((Map<String, Object>) indices.get(index)).get("shards"));
                assertThat(shards.size(), equalTo(2));
                for (String shardId : shards.keySet()) {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> shardStoresStatus = (Map<String, ?>) shards.get(shardId);
                    assertThat(shardStoresStatus.containsKey("stores"), equalTo(true));
                    List<?> stores = (List<?>) shardStoresStatus.get("stores");
                    assertThat(stores.size(), equalTo(storeStatusList.size()));
                    for (int i = 0; i < stores.size(); i++) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> storeInfo = ((Map<String, ?>) stores.get(i));
                        IndicesShardStoresResponse.StoreStatus storeStatus = storeStatusList.get(i);
                        assertThat(((String) storeInfo.get("allocation_id")), equalTo((storeStatus.getAllocationId())));
                        assertThat(storeInfo.containsKey("allocation"), equalTo(true));
                        assertThat(((String) storeInfo.get("allocation")), equalTo(storeStatus.getAllocationStatus().value()));
                        assertThat(storeInfo.containsKey(storeStatus.getNode().getId()), equalTo(true));
                        if (storeStatus.getStoreException() != null) {
                            assertThat(storeInfo.containsKey("store_exception"), equalTo(true));
                        }
                    }
                }
            }
        }
    }

    public void testStoreStatusOrdering() throws Exception {
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        List<IndicesShardStoresResponse.StoreStatus> orderedStoreStatuses = new ArrayList<>();
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(
                node1,
                UUIDs.randomBase64UUID(),
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY,
                null
            )
        );
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(
                node1,
                UUIDs.randomBase64UUID(),
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA,
                null
            )
        );
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(
                node1,
                UUIDs.randomBase64UUID(),
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED,
                null
            )
        );
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(node1, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null)
        );
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(node1, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA, null)
        );
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(node1, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED, null)
        );
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(
                node1,
                UUIDs.randomBase64UUID(),
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA,
                new IOException("corrupted")
            )
        );
        orderedStoreStatuses.add(
            new IndicesShardStoresResponse.StoreStatus(
                node1,
                null,
                IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA,
                new IOException("corrupted")
            )
        );

        List<IndicesShardStoresResponse.StoreStatus> storeStatuses = new ArrayList<>(orderedStoreStatuses);
        Collections.shuffle(storeStatuses, random());
        CollectionUtil.timSort(storeStatuses);
        assertThat(storeStatuses, equalTo(orderedStoreStatuses));
    }
}
