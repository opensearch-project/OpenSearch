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

package org.opensearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ParentChildrenBlockJoinQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.MaxScoreCollector;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.opensearch.search.fetch.subphase.InnerHitsContext.intersect;

/**
 * Query builder for nested queries
 *
 * @opensearch.internal
 */
public class NestedQueryBuilder extends AbstractQueryBuilder<NestedQueryBuilder> {
    public static final String NAME = "nested";
    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    private static final ParseField PATH_FIELD = new ParseField("path");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String path;
    private final ScoreMode scoreMode;
    private final QueryBuilder query;
    private InnerHitBuilder innerHitBuilder;
    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    public NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode) {
        this(path, query, scoreMode, null);
    }

    private NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode, InnerHitBuilder innerHitBuilder) {
        this.path = requireValue(path, "[" + NAME + "] requires 'path' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
        this.scoreMode = requireValue(scoreMode, "[" + NAME + "] requires 'score_mode' field");
        this.innerHitBuilder = innerHitBuilder;
    }

    /**
     * Read from a stream.
     */
    public NestedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readString();
        scoreMode = ScoreMode.values()[in.readVInt()];
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeVInt(scoreMode.ordinal());
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * Returns path of the nested query.
     */
    public String path() {
        return path;
    }

    /**
     * Returns the nested query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */

    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    public NestedQueryBuilder innerHit(InnerHitBuilder innerHitBuilder) {
        this.innerHitBuilder = innerHitBuilder;
        innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        return this;
    }

    /**
     * Returns how the scores from the matching child documents are mapped into the nested parent document.
     */
    public ScoreMode scoreMode() {
        return scoreMode;
    }

    /**
     * Sets whether the query builder should ignore unmapped paths (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     */
    public NestedQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        if (innerHitBuilder != null) {
            innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        }
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(PATH_FIELD.getPreferredName(), path);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        if (scoreMode != null) {
            builder.field(SCORE_MODE_FIELD.getPreferredName(), scoreModeAsString(scoreMode));
        }
        printBoostAndQueryName(builder);
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    public static NestedQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        ScoreMode scoreMode = ScoreMode.Avg;
        String queryName = null;
        QueryBuilder query = null;
        String path = null;
        String currentFieldName = null;
        InnerHitBuilder innerHitBuilder = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    query = parseInnerQueryBuilder(parser);
                } else if (INNER_HITS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    innerHitBuilder = InnerHitBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (PATH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    path = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (SCORE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scoreMode = parseScoreMode(parser.text());
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }
        NestedQueryBuilder queryBuilder = new NestedQueryBuilder(path, query, scoreMode, innerHitBuilder).ignoreUnmapped(ignoreUnmapped)
            .queryName(queryName)
            .boost(boost);
        return queryBuilder;
    }

    public static ScoreMode parseScoreMode(String scoreModeString) {
        if ("none".equals(scoreModeString)) {
            return ScoreMode.None;
        } else if ("min".equals(scoreModeString)) {
            return ScoreMode.Min;
        } else if ("max".equals(scoreModeString)) {
            return ScoreMode.Max;
        } else if ("avg".equals(scoreModeString)) {
            return ScoreMode.Avg;
        } else if ("sum".equals(scoreModeString)) {
            return ScoreMode.Total;
        }
        throw new IllegalArgumentException("No score mode for child query [" + scoreModeString + "] found");
    }

    public static String scoreModeAsString(ScoreMode scoreMode) {
        if (scoreMode == ScoreMode.Total) {
            // Lucene uses 'total' but 'sum' is more consistent with other opensearch APIs
            return "sum";
        } else {
            return scoreMode.name().toLowerCase(Locale.ROOT);
        }
    }

    @Override
    public final String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(NestedQueryBuilder that) {
        return Objects.equals(query, that.query)
            && Objects.equals(path, that.path)
            && Objects.equals(scoreMode, that.scoreMode)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, path, scoreMode, innerHitBuilder, ignoreUnmapped);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[joining] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        ObjectMapper nestedObjectMapper = context.getObjectMapper(path);
        if (nestedObjectMapper == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new IllegalStateException("[" + NAME + "] failed to find nested object under path [" + path + "]");
            }
        }
        if (!nestedObjectMapper.nested().isNested()) {
            throw new IllegalStateException("[" + NAME + "] nested object under path [" + path + "] is not of nested type");
        }
        final BitSetProducer parentFilter;
        Query innerQuery;
        ObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        if (objectMapper == null) {
            parentFilter = context.bitsetFilter(Queries.newNonNestedFilter());
        } else {
            parentFilter = context.bitsetFilter(objectMapper.nestedTypeFilter());
        }

        BitSetProducer previousParentFilter = context.getParentFilter();
        try {
            context.setParentFilter(parentFilter);
            context.nestedScope().nextLevel(nestedObjectMapper);
            try {
                innerQuery = this.query.toQuery(context);
            } finally {
                context.nestedScope().previousLevel();
            }
        } finally {
            context.setParentFilter(previousParentFilter);
        }

        // ToParentBlockJoinQuery requires that the inner query only matches documents
        // in its child space
        if (new NestedHelper(context.getMapperService()).mightMatchNonNestedDocs(innerQuery, path)) {
            innerQuery = Queries.filtered(innerQuery, nestedObjectMapper.nestedTypeFilter());
        }

        return new OpenSearchToParentBlockJoinQuery(
            innerQuery,
            parentFilter,
            scoreMode,
            objectMapper == null ? null : objectMapper.fullPath()
        );
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
        if (rewrittenQuery != query) {
            NestedQueryBuilder nestedQuery = new NestedQueryBuilder(path, rewrittenQuery, scoreMode, innerHitBuilder);
            nestedQuery.ignoreUnmapped(ignoreUnmapped);
            return nestedQuery;
        }
        return this;
    }

    @Override
    public void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        if (innerHitBuilder != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : path;
            if (innerHits.containsKey(name)) {
                throw new IllegalArgumentException("[inner_hits] already contains an entry for key [" + name + "]");
            }

            Map<String, InnerHitContextBuilder> children = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(query, children);
            InnerHitContextBuilder innerHitContextBuilder = new NestedInnerHitContextBuilder(path, query, innerHitBuilder, children);
            innerHits.put(name, innerHitContextBuilder);
        }
    }

    /**
     * Context builder for nested inner hits
     *
     * @opensearch.internal
     */
    static class NestedInnerHitContextBuilder extends InnerHitContextBuilder {
        private final String path;

        NestedInnerHitContextBuilder(
            String path,
            QueryBuilder query,
            InnerHitBuilder innerHitBuilder,
            Map<String, InnerHitContextBuilder> children
        ) {
            super(query, innerHitBuilder, children);
            this.path = path;
        }

        @Override
        protected void doBuild(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException {
            QueryShardContext queryShardContext = parentSearchContext.getQueryShardContext();
            ObjectMapper nestedObjectMapper = queryShardContext.getObjectMapper(path);
            if (nestedObjectMapper == null) {
                if (innerHitBuilder.isIgnoreUnmapped() == false) {
                    throw new IllegalStateException("[" + query.getName() + "] no mapping found for type [" + path + "]");
                } else {
                    return;
                }
            }
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : nestedObjectMapper.fullPath();
            ObjectMapper parentObjectMapper = queryShardContext.nestedScope().getObjectMapper();
            BitSetProducer parentFilter;
            if (parentObjectMapper == null) {
                parentFilter = queryShardContext.bitsetFilter(Queries.newNonNestedFilter());
            } else {
                parentFilter = queryShardContext.bitsetFilter(parentObjectMapper.nestedTypeFilter());
            }
            BitSetProducer previousParentFilter = queryShardContext.getParentFilter();
            try {
                queryShardContext.setParentFilter(parentFilter);
                queryShardContext.nestedScope().nextLevel(nestedObjectMapper);
                queryShardContext.setInnerHitQuery(true);
                try {
                    NestedInnerHitSubContext nestedInnerHits = new NestedInnerHitSubContext(
                        name,
                        parentSearchContext,
                        parentObjectMapper,
                        nestedObjectMapper
                    );
                    setupInnerHitsContext(queryShardContext, nestedInnerHits);
                    innerHitsContext.addInnerHitDefinition(nestedInnerHits);
                } finally {
                    queryShardContext.nestedScope().previousLevel();
                }
            } finally {
                queryShardContext.setParentFilter(previousParentFilter);
                queryShardContext.setInnerHitQuery(false);
            }
        }
    }

    /**
     * Inner hits sub context
     *
     * @opensearch.internal
     */
    static final class NestedInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {

        private final ObjectMapper parentObjectMapper;
        private final ObjectMapper childObjectMapper;

        NestedInnerHitSubContext(String name, SearchContext context, ObjectMapper parentObjectMapper, ObjectMapper childObjectMapper) {
            super(name, context);
            this.parentObjectMapper = parentObjectMapper;
            this.childObjectMapper = childObjectMapper;
        }

        @Override
        public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
            assert seqNoAndPrimaryTerm() == false;
            if (seqNoAndPrimaryTerm) {
                throw new UnsupportedOperationException("nested documents are not assigned sequence numbers");
            }
        }

        @Override
        public TopDocsAndMaxScore topDocs(SearchHit hit) throws IOException {
            Weight innerHitQueryWeight = getInnerHitQueryWeight();

            Query rawParentFilter;
            if (parentObjectMapper == null) {
                rawParentFilter = Queries.newNonNestedFilter();
            } else {
                rawParentFilter = parentObjectMapper.nestedTypeFilter();
            }

            int parentDocId = hit.docId();
            final int readerIndex = ReaderUtil.subIndex(parentDocId, searcher().getIndexReader().leaves());
            // With nested inner hits the nested docs are always in the same segement, so need to use the other segments
            LeafReaderContext ctx = searcher().getIndexReader().leaves().get(readerIndex);

            Query childFilter = childObjectMapper.nestedTypeFilter();
            BitSetProducer parentFilter = context.bitsetFilterCache().getBitSetProducer(rawParentFilter);
            Query q = new ParentChildrenBlockJoinQuery(parentFilter, childFilter, parentDocId);
            Weight weight = context.searcher()
                .createWeight(context.searcher().rewrite(q), org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1f);
            if (size() == 0) {
                TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                return new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(totalHitCountCollector.getTotalHits(), TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                    Float.NaN
                );
            } else {
                int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                TopDocsCollector<?> topDocsCollector;
                MaxScoreCollector maxScoreCollector = null;
                if (sort() != null) {
                    topDocsCollector = TopFieldCollector.create(sort().sort, topN, Integer.MAX_VALUE);
                    if (trackScores()) {
                        maxScoreCollector = new MaxScoreCollector();
                    }
                } else {
                    topDocsCollector = TopScoreDocCollector.create(topN, Integer.MAX_VALUE);
                    maxScoreCollector = new MaxScoreCollector();
                }
                intersect(weight, innerHitQueryWeight, MultiCollector.wrap(topDocsCollector, maxScoreCollector), ctx);
                TopDocs td = topDocsCollector.topDocs(from(), size());
                float maxScore = Float.NaN;
                if (maxScoreCollector != null) {
                    maxScore = maxScoreCollector.getMaxScore();
                }
                return new TopDocsAndMaxScore(td, maxScore);
            }
        }
    }

    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        if (query != null) {
            visitor.getChildVisitor(BooleanClause.Occur.MUST).accept(query);
        }
    }
}
