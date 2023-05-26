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

package org.opensearch.search.slice;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *  A slice builder allowing to split a scroll in multiple partitions.
 *  If the provided field is the "_uid" it uses a {@link org.opensearch.search.slice.TermsSliceQuery}
 *  to do the slicing. The slicing is done at the shard level first and then each shard is split into multiple slices.
 *  For instance if the number of shards is equal to 2 and the user requested 4 slices
 *  then the slices 0 and 2 are assigned to the first shard and the slices 1 and 3 are assigned to the second shard.
 *  This way the total number of bitsets that we need to build on each shard is bounded by the number of slices
 *  (instead of {@code numShards*numSlices}).
 *  Otherwise the provided field must be a numeric and doc_values must be enabled. In that case a
 *  {@link org.opensearch.search.slice.DocValuesSliceQuery} is used to filter the results.
 *
 *  @opensearch.internal
 */
public class SliceBuilder implements Writeable, ToXContentObject {

    private static final DeprecationLogger DEPRECATION_LOG = DeprecationLogger.getLogger(SliceBuilder.class);

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField MAX_FIELD = new ParseField("max");
    private static final ObjectParser<SliceBuilder, Void> PARSER = new ObjectParser<>("slice", SliceBuilder::new);

    static {
        PARSER.declareString(SliceBuilder::setField, FIELD_FIELD);
        PARSER.declareInt(SliceBuilder::setId, ID_FIELD);
        PARSER.declareInt(SliceBuilder::setMax, MAX_FIELD);
    }

    /** Name of field to slice against (_uid by default) */
    private String field = IdFieldMapper.NAME;
    /** The id of the slice */
    private int id = -1;
    /** Max number of slices */
    private int max = -1;

    private SliceBuilder() {}

    public SliceBuilder(int id, int max) {
        this(IdFieldMapper.NAME, id, max);
    }

    /**
     *
     * @param field The name of the field
     * @param id The id of the slice
     * @param max The maximum number of slices
     */
    public SliceBuilder(String field, int id, int max) {
        setField(field);
        setId(id);
        setMax(max);
    }

    public SliceBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.id = in.readVInt();
        this.max = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(id);
        out.writeVInt(max);
    }

    private SliceBuilder setField(String field) {
        if (org.opensearch.core.common.Strings.isEmpty(field)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        this.field = field;
        return this;
    }

    /**
     * The name of the field to slice against
     */
    public String getField() {
        return this.field;
    }

    private SliceBuilder setId(int id) {
        if (id < 0) {
            throw new IllegalArgumentException("id must be greater than or equal to 0");
        }
        if (max != -1 && id >= max) {
            throw new IllegalArgumentException("max must be greater than id");
        }
        this.id = id;
        return this;
    }

    /**
     * The id of the slice.
     */
    public int getId() {
        return id;
    }

    private SliceBuilder setMax(int max) {
        if (max <= 1) {
            throw new IllegalArgumentException("max must be greater than 1");
        }
        if (id != -1 && id >= max) {
            throw new IllegalArgumentException("max must be greater than id");
        }
        this.max = max;
        return this;
    }

    /**
     * The maximum number of slices.
     */
    public int getMax() {
        return max;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder);
        builder.endObject();
        return builder;
    }

    void innerToXContent(XContentBuilder builder) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(MAX_FIELD.getPreferredName(), max);
    }

    public static SliceBuilder fromXContent(XContentParser parser) throws IOException {
        SliceBuilder builder = PARSER.parse(parser, new SliceBuilder(), null);
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SliceBuilder)) {
            return false;
        }

        SliceBuilder o = (SliceBuilder) other;
        return ((field == null && o.field == null) || field.equals(o.field)) && id == o.id && o.max == max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field, this.id, this.max);
    }

    /**
     * Converts this QueryBuilder to a lucene {@link Query}.
     *
     * @param context Additional information needed to build the query
     */
    public Query toFilter(ClusterService clusterService, ShardSearchRequest request, QueryShardContext context, Version minNodeVersion) {
        final MappedFieldType type = context.fieldMapper(field);
        if (type == null) {
            throw new IllegalArgumentException("field " + field + " not found");
        }

        int shardId = request.shardId().id();
        int numShards = context.getIndexSettings().getNumberOfShards();
        if ((request.preference() != null || request.indexRoutings().length > 0)) {
            GroupShardsIterator<ShardIterator> group = buildShardIterator(clusterService, request);
            assert group.size() <= numShards : "index routing shards: "
                + group.size()
                + " cannot be greater than total number of shards: "
                + numShards;
            if (group.size() < numShards) {
                /*
                 * The routing of this request targets a subset of the shards of this index so we need to we retrieve
                 * the original {@link GroupShardsIterator} and compute the request shard id and number of
                 * shards from it.
                 */
                numShards = group.size();
                int ord = 0;
                shardId = -1;
                // remap the original shard id with its index (position) in the sorted shard iterator.
                for (ShardIterator it : group) {
                    assert it.shardId().getIndex().equals(request.shardId().getIndex());
                    if (request.shardId().equals(it.shardId())) {
                        shardId = ord;
                        break;
                    }
                    ++ord;
                }
                assert shardId != -1 : "shard id: " + request.shardId().getId() + " not found in index shard routing";
            }
        }

        String field = this.field;
        boolean useTermQuery = false;
        if ("_uid".equals(field)) {
            throw new IllegalArgumentException("Computing slices on the [_uid] field is illegal for 7.x indices, use [_id] instead");
        } else if (IdFieldMapper.NAME.equals(field)) {
            useTermQuery = true;
        } else if (type.hasDocValues() == false) {
            throw new IllegalArgumentException("cannot load numeric doc values on " + field);
        } else {
            IndexFieldData ifm = context.getForField(type);
            if (ifm instanceof IndexNumericFieldData == false) {
                throw new IllegalArgumentException("cannot load numeric doc values on " + field);
            }
        }

        if (numShards == 1) {
            return useTermQuery ? new TermsSliceQuery(field, id, max) : new DocValuesSliceQuery(field, id, max);
        }
        if (max >= numShards) {
            // the number of slices is greater than the number of shards
            // in such case we can reduce the number of requested shards by slice

            // first we check if the slice is responsible of this shard
            int targetShard = id % numShards;
            if (targetShard != shardId) {
                // the shard is not part of this slice, we can skip it.
                return new MatchNoDocsQuery("this shard is not part of the slice");
            }
            // compute the number of slices where this shard appears
            int numSlicesInShard = max / numShards;
            int rest = max % numShards;
            if (rest > targetShard) {
                numSlicesInShard++;
            }

            if (numSlicesInShard == 1) {
                // this shard has only one slice so we must check all the documents
                return new MatchAllDocsQuery();
            }
            // get the new slice id for this shard
            int shardSlice = id / numShards;

            return useTermQuery
                ? new TermsSliceQuery(field, shardSlice, numSlicesInShard)
                : new DocValuesSliceQuery(field, shardSlice, numSlicesInShard);
        }
        // the number of shards is greater than the number of slices

        // check if the shard is assigned to the slice
        int targetSlice = shardId % max;
        if (id != targetSlice) {
            // the shard is not part of this slice, we can skip it.
            return new MatchNoDocsQuery("this shard is not part of the slice");
        }
        return new MatchAllDocsQuery();
    }

    /**
     * Returns the {@link GroupShardsIterator} for the provided <code>request</code>.
     */
    private GroupShardsIterator<ShardIterator> buildShardIterator(ClusterService clusterService, ShardSearchRequest request) {
        final ClusterState state = clusterService.state();
        String[] indices = new String[] { request.shardId().getIndex().getName() };
        Map<String, Set<String>> routingMap = request.indexRoutings().length > 0
            ? Collections.singletonMap(indices[0], Sets.newHashSet(request.indexRoutings()))
            : null;
        return clusterService.operationRouting().searchShards(state, indices, routingMap, request.preference());
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, true);
    }
}
