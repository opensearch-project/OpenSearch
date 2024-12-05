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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Segment;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Transport response for retrieving indices segment information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndicesSegmentResponse extends BroadcastResponse {

    private final ShardSegments[] shards;

    private Map<String, IndexSegments> indicesSegments;

    IndicesSegmentResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(ShardSegments::new, ShardSegments[]::new);
    }

    IndicesSegmentResponse(
        ShardSegments[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<String, IndexSegments> getIndices() {
        if (indicesSegments != null) {
            return indicesSegments;
        }
        Map<String, IndexSegments> indicesSegments = new HashMap<>();
        if (shards.length == 0) {
            this.indicesSegments = indicesSegments;
            return indicesSegments;
        }

        Arrays.sort(shards, Comparator.comparing(shardSegment -> shardSegment.getShardRouting().getIndexName()));
        int startIndexPos = 0;
        String startIndexName = shards[startIndexPos].getShardRouting().getIndexName();
        for (int i = 0; i < shards.length; i++) {
            if (!shards[i].getShardRouting().getIndexName().equals(startIndexName)) {
                indicesSegments.put(startIndexName, new IndexSegments(startIndexName, Arrays.copyOfRange(shards, startIndexPos, i)));
                startIndexPos = i;
                startIndexName = shards[startIndexPos].getShardRouting().getIndexName();
            }
        }
        // Add the last shardSegment from shards list which would have got missed in the loop above
        indicesSegments.put(startIndexName, new IndexSegments(startIndexName, Arrays.copyOfRange(shards, startIndexPos, shards.length)));

        this.indicesSegments = indicesSegments;
        return indicesSegments;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.INDICES);

        for (IndexSegments indexSegments : getIndices().values()) {
            builder.startObject(indexSegments.getIndex());

            builder.startObject(Fields.SHARDS);
            for (IndexShardSegments indexSegment : indexSegments) {
                builder.startArray(Integer.toString(indexSegment.getShardId().id()));
                for (ShardSegments shardSegments : indexSegment) {
                    builder.startObject();

                    builder.startObject(Fields.ROUTING);
                    builder.field(Fields.STATE, shardSegments.getShardRouting().state());
                    builder.field(Fields.PRIMARY, shardSegments.getShardRouting().primary());
                    builder.field(Fields.NODE, shardSegments.getShardRouting().currentNodeId());
                    if (shardSegments.getShardRouting().relocatingNodeId() != null) {
                        builder.field(Fields.RELOCATING_NODE, shardSegments.getShardRouting().relocatingNodeId());
                    }
                    builder.endObject();

                    builder.field(Fields.NUM_COMMITTED_SEGMENTS, shardSegments.getNumberOfCommitted());
                    builder.field(Fields.NUM_SEARCH_SEGMENTS, shardSegments.getNumberOfSearch());

                    builder.startObject(Fields.SEGMENTS);
                    for (Segment segment : shardSegments) {
                        builder.startObject(segment.getName());
                        builder.field(Fields.GENERATION, segment.getGeneration());
                        builder.field(Fields.NUM_DOCS, segment.getNumDocs());
                        builder.field(Fields.DELETED_DOCS, segment.getDeletedDocs());
                        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, segment.getSize());
                        builder.humanReadableField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, segment.getZeroMemory());
                        builder.field(Fields.COMMITTED, segment.isCommitted());
                        builder.field(Fields.SEARCH, segment.isSearch());
                        if (segment.getVersion() != null) {
                            builder.field(Fields.VERSION, segment.getVersion());
                        }
                        if (segment.isCompound() != null) {
                            builder.field(Fields.COMPOUND, segment.isCompound());
                        }
                        if (segment.getMergeId() != null) {
                            builder.field(Fields.MERGE_ID, segment.getMergeId());
                        }
                        if (segment.getSegmentSort() != null) {
                            toXContent(builder, segment.getSegmentSort());
                        }
                        if (segment.attributes != null && segment.attributes.isEmpty() == false) {
                            builder.field("attributes", segment.attributes);
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }

        builder.endObject();
    }

    private static void toXContent(XContentBuilder builder, Sort sort) throws IOException {
        builder.startArray("sort");
        for (SortField field : sort.getSort()) {
            builder.startObject();
            builder.field("field", field.getField());
            if (field instanceof SortedNumericSortField) {
                builder.field("mode", ((SortedNumericSortField) field).getSelector().toString().toLowerCase(Locale.ROOT));
            } else if (field instanceof SortedSetSortField) {
                builder.field("mode", ((SortedSetSortField) field).getSelector().toString().toLowerCase(Locale.ROOT));
            }
            if (field.getMissingValue() != null) {
                builder.field("missing", field.getMissingValue().toString());
            }
            builder.field("reverse", field.getReverse());
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";

        static final String SEGMENTS = "segments";
        static final String GENERATION = "generation";
        static final String NUM_COMMITTED_SEGMENTS = "num_committed_segments";
        static final String NUM_SEARCH_SEGMENTS = "num_search_segments";
        static final String NUM_DOCS = "num_docs";
        static final String DELETED_DOCS = "deleted_docs";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String COMMITTED = "committed";
        static final String SEARCH = "search";
        static final String VERSION = "version";
        static final String COMPOUND = "compound";
        static final String MERGE_ID = "merge_id";
        static final String MEMORY = "memory";
        static final String MEMORY_IN_BYTES = "memory_in_bytes";
        static final String RAM_TREE = "ram_tree";
        static final String DESCRIPTION = "description";
        static final String CHILDREN = "children";
    }
}
