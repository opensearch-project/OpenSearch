/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import org.apache.lucene.search.SortField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * Sort builder for the pseudo‐field "_shard_doc", which tiebreaks by {@code (shardId << 32) | globalDocId}.
 */
public class ShardDocSortBuilder extends SortBuilder<ShardDocSortBuilder> {

    public static final String NAME = "_shard_doc";

    // parser for JSON: { "_shard_doc": { "order":"asc" } }
    private static final ObjectParser<ShardDocSortBuilder, Void> PARSER = new ObjectParser<>(NAME, ShardDocSortBuilder::new);

    static {
        PARSER.declareString((b, s) -> b.order(SortOrder.fromString(s)), ORDER_FIELD);
    }

    public ShardDocSortBuilder() {
        this.order = SortOrder.ASC; // default to ASC
    }

    public ShardDocSortBuilder(StreamInput in) throws IOException {
        this.order = SortOrder.readFromStream(in);
    }

    public ShardDocSortBuilder(ShardDocSortBuilder other) {
        this.order = other.order;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        order.writeTo(out);
    }

    public static ShardDocSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            token = parser.nextToken();
        }

        switch (token) {
            case START_OBJECT:
                return PARSER.parse(parser, null); // { "_shard_doc": { "order": "asc" } }

            case VALUE_STRING:
            case VALUE_NUMBER:
            case VALUE_BOOLEAN:
            case VALUE_NULL:
                return new ShardDocSortBuilder(); // Scalar shorthand: "_shard_doc" → defaults to ASC

            case START_ARRAY:
                throw new org.opensearch.core.xcontent.XContentParseException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] Expected START_OBJECT or scalar but was: START_ARRAY"
                );

            default:
                throw new org.opensearch.core.xcontent.XContentParseException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] Expected START_OBJECT or scalar but was: " + token
                );
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    protected SortFieldAndFormat build(QueryShardContext context) {
        final int shardId = context.getShardId();
        SortField sf = new SortField(NAME, new ShardDocFieldComparatorSource(shardId), order == SortOrder.DESC);
        return new SortFieldAndFormat(sf, DocValueFormat.RAW);
    }

    @Override
    public BucketedSort buildBucketedSort(QueryShardContext context, int bucketSize, BucketedSort.ExtraData extra) throws IOException {
        throw new UnsupportedOperationException("bucketed sort not supported for " + NAME);
    }

    @Override
    public ShardDocSortBuilder rewrite(QueryRewriteContext ctx) {
        return this;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ShardDocSortBuilder other = (ShardDocSortBuilder) obj;
        return order == other.order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(order);
    }
}
