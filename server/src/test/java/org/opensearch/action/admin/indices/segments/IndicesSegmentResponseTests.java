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

package org.opensearch.action.admin.indices.segments;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Segment;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class IndicesSegmentResponseTests extends OpenSearchTestCase {

    public void testToXContentSerialiationWithSortedFields() throws Exception {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("foo", 0, "node_id", true, ShardRoutingState.STARTED);
        Segment segment = new Segment("my");

        SortField sortField = new SortField("foo", SortField.Type.STRING);
        sortField.setMissingValue(SortField.STRING_LAST);
        segment.segmentSort = new Sort(sortField);

        ShardSegments shardSegments = new ShardSegments(shardRouting, Collections.singletonList(segment));
        IndicesSegmentResponse response = new IndicesSegmentResponse(
            new ShardSegments[] { shardSegments },
            1,
            1,
            0,
            Collections.emptyList()
        );
        try (XContentBuilder builder = jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
    }

    public void testGetIndices() {
        final int totalIndices = 5;
        final int shardsPerIndex = 3;
        final int segmentsPerShard = 2;
        // Preparing a ShardSegments list, which will have (totalIndices * shardsPerIndex) shardSegments.
        // Indices will be named -> foo1, foo2, ..., foo{totalIndices}
        List<ShardSegments> shardSegmentsList = new ArrayList<>();
        for (int indexName = 0; indexName < totalIndices; indexName++) {
            for (int shardId = 0; shardId < shardsPerIndex; shardId++) {
                ShardRouting shardRouting = TestShardRouting.newShardRouting(
                    "foo" + indexName,
                    shardId,
                    "node_id",
                    true,
                    ShardRoutingState.STARTED
                );
                List<Segment> segmentList = new ArrayList<>();
                for (int segmentNum = 0; segmentNum < segmentsPerShard; segmentNum++) {
                    segmentList.add(new Segment("foo" + indexName + shardId + segmentNum));
                }
                shardSegmentsList.add(new ShardSegments(shardRouting, segmentList));
            }
        }
        Collections.shuffle(shardSegmentsList);

        // Prepare the IndicesSegmentResponse object and get the indicesSegments map
        IndicesSegmentResponse response = new IndicesSegmentResponse(
            shardSegmentsList.toArray(new ShardSegments[0]),
            totalIndices * shardsPerIndex,
            totalIndices * shardsPerIndex,
            0,
            Collections.emptyList()
        );
        Map<String, IndexSegments> indicesSegments = response.getIndices();

        assertEquals(totalIndices, indicesSegments.size());
        for (Map.Entry<String, IndexSegments> indexSegmentEntry : indicesSegments.entrySet()) {
            assertEquals(shardsPerIndex, indexSegmentEntry.getValue().getShards().size());
            for (IndexShardSegments indexShardSegment : indexSegmentEntry.getValue().getShards().values()) {
                for (ShardSegments shardSegment : indexShardSegment.getShards()) {
                    assertEquals(segmentsPerShard, shardSegment.getSegments().size());
                    for (int i = 0; i < segmentsPerShard; i++) {
                        String segmentName = indexSegmentEntry.getKey() + shardSegment.getShardRouting().getId() + i;
                        assertEquals(segmentName, shardSegment.getSegments().get(i).getName());
                    }
                }
            }
        }
    }
}
