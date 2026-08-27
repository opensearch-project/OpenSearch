/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortFieldAndFormat;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Coordinator-side, serializable companion to {@link RankDocsSortField} — the sort analogue of
 * {@link RankDocsQueryBuilder}. The retriever framework injects this on the final search's {@code sort}
 * when the resolved root's order is decoupled from {@code _score} (e.g. {@code pinned}) and the user gave
 * no top-level sort. It carries the whole resolved window and is broadcast to every shard; {@link #build}
 * then runs <em>on each shard</em> — where {@link QueryShardContext#getShardId()} is available — scopes
 * the window to that shard's {@code (index, shardId)}, and produces the shard-local {@link RankDocsSortField}.
 * <p>
 * This split is why the shard id is knowable at all: the coordinator cannot know a specific shard, so it
 * only builds this spec; the {@code shardId} is resolved per shard in {@link #build}, exactly as
 * {@link RankDocsQueryBuilder#doToQuery} does for the query.
 * <p>
 * Like {@link RankDocsQueryBuilder}, this is purely internal to the retriever framework: it is registered
 * so it serializes to data nodes, but it cannot be authored directly in a {@code _search} body —
 * {@link #fromXContent} always rejects, and it is only ever reconstructed on data nodes over the binary
 * {@link StreamInput} path.
 *
 * @opensearch.internal
 */
public final class RankDocsSortBuilder extends SortBuilder<RankDocsSortBuilder> {

    public static final String NAME = "rank_docs_sort";

    private final List<RankDoc> rankDocs;

    public RankDocsSortBuilder(List<RankDoc> rankDocs) {
        // The retriever executor sets an immutable resolved window; nothing mutates it after, so no copy.
        this.rankDocs = Objects.requireNonNull(rankDocs, "rankDocs");
    }

    public RankDocsSortBuilder(StreamInput in) throws IOException {
        this.rankDocs = in.readList(RankDoc::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(rankDocs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(NAME);
        for (RankDoc rd : rankDocs) {
            rd.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Always rejects: {@code rank_docs_sort} is internal to the retriever framework and cannot be authored
     * in a {@code _search} body. It is only ever built in-process on the coordinator and transported to data
     * nodes over the binary {@link StreamInput}/{@link StreamOutput} path (see {@link #RankDocsSortBuilder(StreamInput)}),
     * never parsed from XContent. This method exists solely to satisfy the {@code SortSpec} registration
     * contract, which requires a parser.
     */
    public static RankDocsSortBuilder fromXContent(XContentParser parser, String fieldName) {
        throw new ParsingException(
            parser.getTokenLocation(),
            "[" + NAME + "] sort is internal to the retriever framework and cannot be used directly in a search request"
        );
    }

    @Override
    protected SortFieldAndFormat build(QueryShardContext context) {
        final String indexName = context.index().getName();
        final int shardId = context.getShardId();
        // Scope the broadcast window to this shard HERE (shard id is only available on the shard), so the
        // RankDocsSortField only carries — and only resolves — the docs that can exist on this shard.
        return new SortFieldAndFormat(
            new RankDocsSortField(RankDocsQueryBuilder.filterToShard(rankDocs, indexName, shardId), indexName, shardId),
            DocValueFormat.RAW
        );
    }

    @Override
    public BucketedSort buildBucketedSort(QueryShardContext context, int bucketSize, BucketedSort.ExtraData extra) {
        throw new UnsupportedOperationException("bucketed sort not supported for " + NAME);
    }

    @Override
    public RankDocsSortBuilder rewrite(QueryRewriteContext ctx) {
        return this;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return rankDocs.equals(((RankDocsSortBuilder) o).rankDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rankDocs);
    }
}
