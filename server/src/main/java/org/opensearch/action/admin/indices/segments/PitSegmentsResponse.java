/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segments;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.Accountable;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.engine.Segment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class PitSegmentsResponse extends BroadcastResponse {

    private final ShardSegments[] shards;

    private Map<String, IndexSegments> indicesSegments;

    PitSegmentsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(ShardSegments::new, ShardSegments[]::new);
    }

    PitSegmentsResponse(ShardSegments[] shards, int totalShards, int successfulShards, int failedShards,
                           List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<String, IndexSegments> getIndices() {
        if (indicesSegments != null) {
            return indicesSegments;
        }
        Map<String, IndexSegments> indicesSegments = new HashMap<>();

        Set<String> indices = new HashSet<>();
        for (ShardSegments shard : shards) {
            indices.add(shard.getShardRouting().getIndexName());
        }

        for (String indexName : indices) {
            List<ShardSegments> shards = new ArrayList<>();
            for (ShardSegments shard : this.shards) {
                if (shard.getShardRouting().getIndexName().equals(indexName)) {
                    shards.add(shard);
                }
            }
            indicesSegments.put(indexName, new IndexSegments(indexName, shards.toArray(new ShardSegments[shards.size()])));
        }
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
        builder.startObject(PitSegmentsResponse.Fields.INDICES);

        for (IndexSegments indexSegments : getIndices().values()) {
            builder.startObject(indexSegments.getIndex());

            builder.startObject(PitSegmentsResponse.Fields.SHARDS);
            for (IndexShardSegments indexSegment : indexSegments) {
                builder.startArray(Integer.toString(indexSegment.getShardId().id()));
                for (ShardSegments shardSegments : indexSegment) {
                    builder.startObject();

                    builder.startObject(PitSegmentsResponse.Fields.ROUTING);
                    builder.field(PitSegmentsResponse.Fields.STATE, shardSegments.getShardRouting().state());
                    builder.field(PitSegmentsResponse.Fields.PRIMARY, shardSegments.getShardRouting().primary());
                    builder.field(PitSegmentsResponse.Fields.NODE, shardSegments.getShardRouting().currentNodeId());
                    if (shardSegments.getShardRouting().relocatingNodeId() != null) {
                        builder.field(PitSegmentsResponse.Fields.RELOCATING_NODE, shardSegments.getShardRouting().relocatingNodeId());
                    }
                    builder.endObject();

                    builder.field(PitSegmentsResponse.Fields.NUM_COMMITTED_SEGMENTS, shardSegments.getNumberOfCommitted());
                    builder.field(PitSegmentsResponse.Fields.NUM_SEARCH_SEGMENTS, shardSegments.getNumberOfSearch());

                    builder.startObject(PitSegmentsResponse.Fields.SEGMENTS);
                    for (Segment segment : shardSegments) {
                        builder.startObject(segment.getName());
                        builder.field(PitSegmentsResponse.Fields.GENERATION, segment.getGeneration());
                        builder.field(PitSegmentsResponse.Fields.NUM_DOCS, segment.getNumDocs());
                        builder.field(PitSegmentsResponse.Fields.DELETED_DOCS, segment.getDeletedDocs());
                        builder.humanReadableField(PitSegmentsResponse.Fields.SIZE_IN_BYTES, PitSegmentsResponse.Fields.SIZE, segment.getSize());
                        builder.humanReadableField(PitSegmentsResponse.Fields.MEMORY_IN_BYTES, PitSegmentsResponse.Fields.MEMORY, new ByteSizeValue(segment.getMemoryInBytes()));
                        builder.field(PitSegmentsResponse.Fields.COMMITTED, segment.isCommitted());
                        builder.field(PitSegmentsResponse.Fields.SEARCH, segment.isSearch());
                        if (segment.getVersion() != null) {
                            builder.field(PitSegmentsResponse.Fields.VERSION, segment.getVersion());
                        }
                        if (segment.isCompound() != null) {
                            builder.field(PitSegmentsResponse.Fields.COMPOUND, segment.isCompound());
                        }
                        if (segment.getMergeId() != null) {
                            builder.field(PitSegmentsResponse.Fields.MERGE_ID, segment.getMergeId());
                        }
                        if (segment.getSegmentSort() != null) {
                            toXContent(builder, segment.getSegmentSort());
                        }
                        if (segment.ramTree != null) {
                            builder.startArray(PitSegmentsResponse.Fields.RAM_TREE);
                            for (Accountable child : segment.ramTree.getChildResources()) {
                                toXContent(builder, child);
                            }
                            builder.endArray();
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
                builder.field("mode", ((SortedNumericSortField) field).getSelector()
                    .toString().toLowerCase(Locale.ROOT));
            } else if (field instanceof SortedSetSortField) {
                builder.field("mode", ((SortedSetSortField) field).getSelector()
                    .toString().toLowerCase(Locale.ROOT));
            }
            if (field.getMissingValue() != null) {
                builder.field("missing", field.getMissingValue().toString());
            }
            builder.field("reverse", field.getReverse());
            builder.endObject();
        }
        builder.endArray();
    }

    private static void toXContent(XContentBuilder builder, Accountable tree) throws IOException {
        builder.startObject();
        builder.field(PitSegmentsResponse.Fields.DESCRIPTION, tree.toString());
        builder.humanReadableField(PitSegmentsResponse.Fields.SIZE_IN_BYTES, PitSegmentsResponse.Fields.SIZE, new ByteSizeValue(tree.ramBytesUsed()));
        Collection<Accountable> children = tree.getChildResources();
        if (children.isEmpty() == false) {
            builder.startArray(PitSegmentsResponse.Fields.CHILDREN);
            for (Accountable child : children) {
                toXContent(builder, child);
            }
            builder.endArray();
        }
        builder.endObject();
    }

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
