/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Internal query builder that carries a resolved retriever window onto a real {@code _search} and, on
 * each shard, turns it into a {@link RankDocsQuery} scoped to that shard's {@code (index, shardId)}.
 * <p>
 * This is intentionally minimal: it exists so a resolved ranking can ride the ordinary search path
 * (and be inspected with the profile API). The retriever framework always builds it in-process, so it
 * is registered as a named query and always serializes to data nodes. It is purely internal — it cannot
 * be authored directly in a {@code _search} body: {@link #fromXContent} always rejects, and the query is
 * only ever reconstructed on data nodes over the binary {@link StreamInput} path.
 * <p>
 * JSON shape (rendered by {@link #doXContent} for the profile API / debugging only):
 * <pre>
 * { "rank_docs": {
 *     "docs": [ { "index": "products", "shard": 0, "id": "a", "score": 0.9, "position": 0 }, ... ]
 * } }
 * </pre>
 *
 * @opensearch.internal
 */
public final class RankDocsQueryBuilder extends AbstractQueryBuilder<RankDocsQueryBuilder> {

    public static final String NAME = "rank_docs";

    private final List<RankDoc> rankDocs;

    public RankDocsQueryBuilder(List<RankDoc> rankDocs) {
        // The retriever executor sets an immutable resolved window; nothing mutates it after, so no copy.
        this.rankDocs = Objects.requireNonNull(rankDocs, "rankDocs");
    }

    public RankDocsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.rankDocs = in.readList(RankDoc::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeCollection(rankDocs);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("docs");
        for (RankDoc rd : rankDocs) {
            rd.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /**
     * Always rejects: {@code rank_docs} is internal to the retriever framework and cannot be authored in a
     * {@code _search} body. The query is only ever built in-process on the coordinator and transported to
     * data nodes over the binary {@link StreamInput}/{@link StreamOutput} path (see {@link #RankDocsQueryBuilder(StreamInput)}),
     * never parsed from XContent. This method exists solely to satisfy the {@code QuerySpec} registration
     * contract, which requires a parser.
     */
    public static RankDocsQueryBuilder fromXContent(XContentParser parser) {
        throw new ParsingException(
            parser.getTokenLocation(),
            "[" + NAME + "] query is internal to the retriever framework and cannot be used directly in a search request"
        );
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        final String indexName = context.index().getName();
        final int shardId = context.getShardId();
        // Filter the window down to this shard's (index, shardId) HERE, before building the query,
        // so RankDocsQuery only holds — and only tries to resolve — the docs that can exist on this shard.
        return new RankDocsQuery(filterToShard(rankDocs, indexName, shardId), indexName, shardId);
    }

    /**
     * The subset of {@code window} whose docs belong to {@code (indexName, shardId)}, in order. Returns a
     * freshly-built list (no defensive copy needed — it is not retained here). Package-visible for testing
     * the per-shard scoping independently of a {@link QueryShardContext}.
     */
    static List<RankDoc> filterToShard(List<RankDoc> window, String indexName, int shardId) {
        final List<RankDoc> mine = new ArrayList<>();
        for (RankDoc rd : window) {
            if (rd.belongsTo(indexName, shardId)) {
                mine.add(rd);
            }
        }
        return mine;
    }

    @Override
    protected boolean doEquals(RankDocsQueryBuilder other) {
        return rankDocs.equals(other.rankDocs);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(rankDocs);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
