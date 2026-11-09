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

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;

public class BulkShardRequestTests extends OpenSearchTestCase {
    public void testToString() {
        String index = randomSimpleString(random(), 10);
        int count = between(2, 100);
        final ShardId shardId = new ShardId(index, "ignored", 0);
        BulkShardRequest r = new BulkShardRequest(shardId, RefreshPolicy.NONE, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0]", r.getDescription());

        r = new BulkShardRequest(shardId, RefreshPolicy.IMMEDIATE, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests and a refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[IMMEDIATE]", r.getDescription());

        r = new BulkShardRequest(shardId, RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests blocking until refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[WAIT_UNTIL]", r.getDescription());
    }

    public void testBulkShardRequestSerialization() throws IOException {
        final String index = randomSimpleString(random(), 10);
        final int count = between(2, 100);
        final ShardId shardId = new ShardId(index, "ignored", 0);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final BulkShardRequest expected = new BulkShardRequest(shardId, refreshPolicy, generateBulkItemRequests(count));

        final BytesStreamOutput out = new BytesStreamOutput();

        expected.writeTo(out);

        final BulkShardRequest actual = new BulkShardRequest(out.bytes().streamInput());

        assertEquals(expected.getParentTask().getId(), actual.getParentTask().getId());
        assertEquals(expected.getParentTask().getNodeId(), actual.getParentTask().getNodeId());

        assertEquals(expected.shardId(), actual.shardId());
        assertEquals(expected.waitForActiveShards(), actual.waitForActiveShards());
        assertEquals(expected.timeout(), actual.timeout());
        assertEquals(expected.index(), actual.index());
        assertEquals(expected.routedBasedOnClusterVersion(), actual.routedBasedOnClusterVersion());

        assertEquals(expected.getRefreshPolicy(), actual.getRefreshPolicy());

        assertEquals(expected.items().length, actual.items().length);
        for (int i = 0; i < count; ++i) {
            final BulkItemRequest expectedItem = expected.items()[i];
            final BulkItemRequest actualItem = actual.items()[i];
            if (null == expectedItem) {
                assertNull(actualItem);
                continue;
            }
            assertEquals(expectedItem.id(), actualItem.id());
            assertEquals(expectedItem.request().id(), actualItem.request().id());
            assertEquals(expectedItem.request().index(), actualItem.request().index());
            assertEquals(expectedItem.request().opType(), actualItem.request().opType());
        }
    }

    private BulkItemRequest[] generateBulkItemRequests(final int count) {
        final BulkItemRequest[] items = new BulkItemRequest[count];
        final int nullIdx = randomIntBetween(0, count - 1);
        for (int i = 0; i < count; i++) {
            if (i == nullIdx) {
                items[i] = null;
                continue;
            }
            final DocWriteRequest<?> request = switch (randomFrom(DocWriteRequest.OpType.values())) {
                case INDEX -> new IndexRequest("index").id("id_" + i);
                case CREATE -> new IndexRequest("index").id("id_" + i).create(true);
                case UPDATE -> new UpdateRequest("index", "id_" + i);
                case DELETE -> new DeleteRequest("index", "id_" + i);
            };
            items[i] = new BulkItemRequest(i, request);
        }
        return items;
    }

    public void testSetPrimaryResponses() {
        final String index = randomSimpleString(random(), 1, 10);
        final int count = between(2, 100);
        final ShardId shardId = new ShardId(index, randomAlphaOfLength(10), randomIntBetween(0, 5));
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        final BulkItemRequest[] items = generateBulkItemRequests(count);

        final BulkShardRequest original = new BulkShardRequest(shardId, refreshPolicy, items);

        // Set random mutable properties from ReplicationRequest
        original.timeout(TimeValue.timeValueMillis(randomLongBetween(1, 60000)));
        original.waitForActiveShards(ActiveShardCount.from(randomIntBetween(0, 10)));
        original.routedBasedOnClusterVersion(randomNonNegativeLong());
        original.setParentTask(new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()));

        // Build primary responses for each item
        final BulkItemResponse[] primaryResponses = new BulkItemResponse[count];
        for (int i = 0; i < count; i++) {
            if (items[i] != null) {
                primaryResponses[i] = new BulkItemResponse(
                    i,
                    items[i].request().opType(),
                    new IndexResponse(shardId, "id_" + i, 1, 1, 1, true)
                );
            }
        }

        final BulkShardRequest cloned = original.setPrimaryResponses(primaryResponses);

        // Verify all cloned properties match
        assertEquals(original.shardId(), cloned.shardId());
        assertEquals(original.getRefreshPolicy(), cloned.getRefreshPolicy());
        assertEquals(original.timeout(), cloned.timeout());
        assertEquals(original.waitForActiveShards(), cloned.waitForActiveShards());
        assertEquals(original.routedBasedOnClusterVersion(), cloned.routedBasedOnClusterVersion());
        assertEquals(original.getParentTask(), cloned.getParentTask());

        // Verify items have primary responses attached
        assertEquals(count, cloned.items().length);
        for (int i = 0; i < count; i++) {
            if (items[i] == null) {
                assertNull(cloned.items()[i]);
            } else {
                assertEquals(items[i].id(), cloned.items()[i].id());
                assertEquals(items[i].request().opType(), cloned.items()[i].request().opType());
                assertSame(primaryResponses[i], cloned.items()[i].primaryResponse());
            }
        }
    }
}
