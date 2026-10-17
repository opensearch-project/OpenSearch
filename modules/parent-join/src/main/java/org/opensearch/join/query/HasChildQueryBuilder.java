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

package org.opensearch.join.query;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.join.mapper.ParentIdFieldMapper;
import org.opensearch.join.mapper.ParentJoinFieldMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * A query builder for {@code has_child} query.
 */
public class HasChildQueryBuilder extends AbstractQueryBuilder<HasChildQueryBuilder> {
    public static final String NAME = "has_child";

    /**
     * The default maximum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MAX_CHILDREN = Integer.MAX_VALUE;
    /**
     * The default minimum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MIN_CHILDREN = 1;

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;
    static final String MIN_CHILDREN_0_DEPRECATION_MESSAGE = "[min_children] 0 will be rejected "
        + "starting in 8.0. Using 1 instead of 0 will return the same result set.";
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField MAX_CHILDREN_FIELD = new ParseField("max_children");
    private static final ParseField MIN_CHILDREN_FIELD = new ParseField("min_children");
    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");
    /**
     * Optional join-field term prefix used to build a group-scoped global ordinals map, avoiding the shard-wide
     * global-ordinals rewrite. This is an optimisation for indices that store documents from many groups in the same
     * index, whose document ids are prefixed with a group id, and that issue group-scoped {@code has_child} queries.
     * See {@link org.opensearch.index.fielddata.ordinals.GlobalOrdinalsBuilder#buildScoped}.
     */
    private static final ParseField SCOPE_PREFIX_FIELD = new ParseField("scope_prefix");
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(HasChildQueryBuilder.class);
    private final QueryBuilder query;
    private final String type;
    private final ScoreMode scoreMode;
    private InnerHitBuilder innerHitBuilder;
    private int minChildren = DEFAULT_MIN_CHILDREN;
    private int maxChildren = DEFAULT_MAX_CHILDREN;
    private boolean ignoreUnmapped = false;
    private String scopePrefix = null;

    public HasChildQueryBuilder(String type, QueryBuilder query, ScoreMode scoreMode) {
        this(type, query, DEFAULT_MIN_CHILDREN, DEFAULT_MAX_CHILDREN, scoreMode, null);
    }

    private HasChildQueryBuilder(
        String type,
        QueryBuilder query,
        int minChildren,
        int maxChildren,
        ScoreMode scoreMode,
        InnerHitBuilder innerHitBuilder
    ) {
        this.type = requireValue(type, "[" + NAME + "] requires 'type' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
        this.scoreMode = requireValue(scoreMode, "[" + NAME + "] requires 'score_mode' field");
        this.innerHitBuilder = innerHitBuilder;
        this.minChildren = minChildren;
        this.maxChildren = maxChildren;
    }

    /**
     * Read from a stream.
     */
    public HasChildQueryBuilder(StreamInput in) throws IOException {
        super(in);
        type = in.readString();
        minChildren = in.readInt();
        maxChildren = in.readInt();
        scoreMode = ScoreMode.values()[in.readVInt()];
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_3_10_0)) {
            scopePrefix = in.readOptionalString();
        } else {
            scopePrefix = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeInt(minChildren);
        out.writeInt(maxChildren);
        out.writeVInt(scoreMode.ordinal());
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeBoolean(ignoreUnmapped);
        if (out.getVersion().onOrAfter(Version.V_3_10_0)) {
            out.writeOptionalString(scopePrefix);
        } else if (scopePrefix != null) {
            throw new IllegalArgumentException(
                "[has_child] scope_prefix is not supported on nodes with version [" + out.getVersion() + "], which is before 3.10.0"
            );
        }
    }

    /**
     * Sets an optional join-field term prefix (e.g. {@code "<groupId>:"}) used to build a group-scoped global
     * ordinals map instead of the shard-wide one. The caller must ensure the query is also filtered to the same
     * prefix, otherwise matches outside the prefix will be missed.
     */
    public HasChildQueryBuilder scopePrefix(String scopePrefix) {
        this.scopePrefix = scopePrefix;
        return this;
    }

    public String scopePrefix() {
        return scopePrefix;
    }

    /**
     * Defines the minimum number of children that are required to match for the parent to be considered a match and
     * the maximum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder minMaxChildren(int minChildren, int maxChildren) {
        if (minChildren < 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires non-negative 'min_children' field");
        }
        if (minChildren == 0) {
            deprecationLogger.deprecate("min_children", MIN_CHILDREN_0_DEPRECATION_MESSAGE);
        }
        if (maxChildren < 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires non-negative 'max_children' field");
        }
        if (maxChildren < minChildren) {
            throw new IllegalArgumentException("[" + NAME + "] 'max_children' is less than 'min_children'");
        }
        this.minChildren = minChildren;
        this.maxChildren = maxChildren;
        return this;
    }

    /**
     * Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    public HasChildQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHitBuilder = innerHit;
        innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        return this;
    }

    /**
     * Returns the children query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns the child type
     */
    public String childType() {
        return type;
    }

    /**
     * Returns how the scores from the matching child documents are mapped into the parent document.
     */
    public ScoreMode scoreMode() {
        return scoreMode;
    }

    /**
     * Returns the minimum number of children that are required to match for the parent to be considered a match.
     * The default is {@value #DEFAULT_MIN_CHILDREN}
     */
    public int minChildren() {
        return minChildren;
    }

    /**
     * Returns the maximum number of children that are required to match for the parent to be considered a match.
     * The default is {@value #DEFAULT_MAX_CHILDREN}
     */
    public int maxChildren() {
        return maxChildren;
    }

    /**
     * Sets whether the query builder should ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public HasChildQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        if (innerHitBuilder != null) {
            innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        }
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(SCORE_MODE_FIELD.getPreferredName(), NestedQueryBuilder.scoreModeAsString(scoreMode));
        builder.field(MIN_CHILDREN_FIELD.getPreferredName(), minChildren);
        builder.field(MAX_CHILDREN_FIELD.getPreferredName(), maxChildren);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        if (scopePrefix != null) {
            builder.field(SCOPE_PREFIX_FIELD.getPreferredName(), scopePrefix);
        }
        printBoostAndQueryName(builder);
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    public static HasChildQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String childType = null;
        ScoreMode scoreMode = ScoreMode.None;
        int minChildren = HasChildQueryBuilder.DEFAULT_MIN_CHILDREN;
        int maxChildren = HasChildQueryBuilder.DEFAULT_MAX_CHILDREN;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        String queryName = null;
        String scopePrefix = null;
        InnerHitBuilder innerHitBuilder = null;
        String currentFieldName = null;
        XContentParser.Token token;
        QueryBuilder iqb = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    iqb = parseInnerQueryBuilder(parser);
                } else if (INNER_HITS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    innerHitBuilder = InnerHitBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    childType = parser.text();
                } else if (SCORE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scoreMode = NestedQueryBuilder.parseScoreMode(parser.text());
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (MIN_CHILDREN_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minChildren = parser.intValue(true);
                } else if (MAX_CHILDREN_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxChildren = parser.intValue(true);
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (SCOPE_PREFIX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scopePrefix = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            }
        }
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(childType, iqb, scoreMode);
        hasChildQueryBuilder.minMaxChildren(minChildren, maxChildren);
        hasChildQueryBuilder.queryName(queryName);
        hasChildQueryBuilder.boost(boost);
        hasChildQueryBuilder.ignoreUnmapped(ignoreUnmapped);
        hasChildQueryBuilder.scopePrefix(scopePrefix);
        if (innerHitBuilder != null) {
            hasChildQueryBuilder.innerHit(innerHitBuilder);
        }
        return hasChildQueryBuilder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[joining] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        ParentJoinFieldMapper joinFieldMapper = ParentJoinFieldMapper.getMapper(context.getMapperService());
        if (joinFieldMapper == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "[" + NAME + "] no join field has been configured");
            }
        }

        ParentIdFieldMapper parentIdFieldMapper = joinFieldMapper.getParentIdFieldMapper(type, false);
        if (parentIdFieldMapper != null) {
            Query parentFilter = parentIdFieldMapper.getParentFilter();
            Query childFilter = parentIdFieldMapper.getChildFilter(type);
            MappedFieldType fieldType = parentIdFieldMapper.fieldType();
            Query innerQuery = Queries.filtered(query.toQuery(context), childFilter);
            if (scopePrefix != null) {
                // When ordinals are group-scoped, only children whose parent-join key starts with the
                // prefix have a slot in the scoped OrdinalMap. Restrict the collected children to that
                // prefix as well, otherwise out-of-prefix children resolve to global ord -1 and the join's
                // GlobalOrdinalsCollector throws (LongBitSet.set(-1)). The parent-id field is a keyword-like
                // field whose value is the parent doc id ("<groupId>:<childId>"), so a prefix query is
                // exactly the group filter we need.
                Query childPrefixFilter = new PrefixQuery(new Term(fieldType.name(), new BytesRef(scopePrefix)));
                innerQuery = new BooleanQuery.Builder().add(innerQuery, BooleanClause.Occur.MUST)
                    .add(childPrefixFilter, BooleanClause.Occur.FILTER)
                    .build();
            }
            final SortedSetOrdinalsIndexFieldData fieldData = context.getForField(fieldType);
            return new LateParsingQuery(
                parentFilter,
                innerQuery,
                minChildren(),
                maxChildren(),
                fieldType.name(),
                scoreMode,
                fieldData,
                context.getSearchSimilarity(),
                scopePrefix
            );
        } else {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(
                    context,
                    "[" + NAME + "] join field [" + joinFieldMapper.name() + "] doesn't hold [" + type + "] as a child"
                );
            }
        }
    }

    /**
     * A query that rewrites into another query using
     * {@link JoinUtil#createJoinQuery(String, Query, Query, IndexSearcher, ScoreMode, OrdinalMap, int, int)}
     * that executes the actual join.
     * <p>
     * This query is exclusively used by the {@link HasChildQueryBuilder} and {@link HasParentQueryBuilder} to get access
     * to the {@link DirectoryReader} used by the current search in order to retrieve the {@link OrdinalMap}.
     * The {@link OrdinalMap} is required by {@link JoinUtil} to execute the join.
     */
    // TODO: Find a way to remove this query and let doToQuery(...) just return the query from JoinUtil.createJoinQuery(...)
    public static final class LateParsingQuery extends Query {

        private final Query toQuery;
        private final Query innerQuery;
        private final int minChildren;
        private final int maxChildren;
        private final String joinField;
        private final ScoreMode scoreMode;
        private final SortedSetOrdinalsIndexFieldData fieldDataJoin;
        private final Similarity similarity;
        private final String scopePrefix;

        LateParsingQuery(
            Query toQuery,
            Query innerQuery,
            int minChildren,
            int maxChildren,
            String joinField,
            ScoreMode scoreMode,
            SortedSetOrdinalsIndexFieldData fieldData,
            Similarity similarity,
            String scopePrefix
        ) {
            this.toQuery = toQuery;
            this.innerQuery = innerQuery;
            this.minChildren = minChildren;
            this.maxChildren = maxChildren;
            this.joinField = joinField;
            this.scoreMode = scoreMode;
            this.fieldDataJoin = fieldData;
            this.similarity = similarity;
            this.scopePrefix = scopePrefix;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            Query rewritten = super.rewrite(searcher);
            if (rewritten != this) {
                return rewritten;
            }
            IndexReader reader = searcher.getIndexReader();
            if (reader instanceof DirectoryReader) {
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                indexSearcher.setQueryCache(null);
                indexSearcher.setSimilarity(similarity);
                final IndexOrdinalsFieldData indexParentChildFieldData;
                if (scopePrefix != null) {
                    // Group-scoped global ordinals: a map over only the "<scopePrefix>*" join terms, which is
                    // dramatically cheaper on large shards. Now served from a dedicated node-level scoped cache
                    // (keyed by field + reader-generation + prefix) so a burst of has_child legs/queries for the
                    // same group reuses one build until the next refresh — instead of rebuilding the OrdinalMap on
                    // the search thread for every clause, which caused the periodic latency spikes. The inner/parent
                    // filters are already restricted to the same group, so out-of-prefix docs are never collected.
                    // Breaker bytes are owned and released by the cache on eviction (reader close / refresh).
                    indexParentChildFieldData = fieldDataJoin.loadGlobalScoped(
                        (DirectoryReader) reader,
                        new org.apache.lucene.util.BytesRef(scopePrefix)
                    );
                } else {
                    indexParentChildFieldData = fieldDataJoin.loadGlobal((DirectoryReader) reader);
                }
                OrdinalMap ordinalMap = indexParentChildFieldData.getOrdinalMap();
                if (scopePrefix != null && ordinalMap.getValueCount() == 0) {
                    // The group-scoped ordinal map contains no join terms for this shard/segment set (i.e. this
                    // shard holds no documents whose join key starts with the prefix). An empty OrdinalMap cannot
                    // back a join query (JoinUtil indexes into per-segment ordinal structures), so short-circuit
                    // to "match nothing here" — correct, since there are no parents for this group on this shard.
                    return Queries.newMatchNoDocsQueryWithoutRewrite("no join terms for scope_prefix [" + scopePrefix + "] on this shard");
                }
                return JoinUtil.createJoinQuery(
                    joinField,
                    innerQuery,
                    toQuery,
                    indexSearcher,
                    scoreMode,
                    ordinalMap,
                    minChildren,
                    maxChildren
                );
            } else {
                if (reader.leaves().isEmpty() && reader.numDocs() == 0) {
                    // asserting reader passes down a MultiReader during rewrite which makes this
                    // blow up since for this query to work we have to have a DirectoryReader otherwise
                    // we can't load global ordinals - for this to work we simply check if the reader has no leaves
                    // and rewrite to match nothing
                    return Queries.newMatchNoDocsQueryWithoutRewrite("unable to load global ordinals with an empty directory reader");
                }
                throw new IllegalStateException(
                    "can't load global ordinals for reader of type: " + reader.getClass() + " must be a DirectoryReader"
                );
            }
        }

        @Override
        public boolean equals(Object o) {
            if (sameClassAs(o) == false) return false;

            LateParsingQuery that = (LateParsingQuery) o;

            if (minChildren != that.minChildren) return false;
            if (maxChildren != that.maxChildren) return false;
            if (!toQuery.equals(that.toQuery)) return false;
            if (!innerQuery.equals(that.innerQuery)) return false;
            if (!joinField.equals(that.joinField)) return false;
            if (!Objects.equals(scopePrefix, that.scopePrefix)) return false;
            return scoreMode == that.scoreMode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), toQuery, innerQuery, minChildren, maxChildren, joinField, scoreMode, scopePrefix);
        }

        @Override
        public String toString(String s) {
            return "LateParsingQuery {joinField=" + joinField + "}";
        }

        public int getMinChildren() {
            return minChildren;
        }

        public int getMaxChildren() {
            return maxChildren;
        }

        public ScoreMode getScoreMode() {
            return scoreMode;
        }

        public Query getInnerQuery() {
            return innerQuery;
        }

        public Similarity getSimilarity() {
            return similarity;
        }
    }

    @Override
    protected boolean doEquals(HasChildQueryBuilder that) {
        return Objects.equals(query, that.query)
            && Objects.equals(type, that.type)
            && Objects.equals(scoreMode, that.scoreMode)
            && Objects.equals(minChildren, that.minChildren)
            && Objects.equals(maxChildren, that.maxChildren)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(ignoreUnmapped, that.ignoreUnmapped)
            && Objects.equals(scopePrefix, that.scopePrefix);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, scoreMode, minChildren, maxChildren, innerHitBuilder, ignoreUnmapped, scopePrefix);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryShardContext);
        if (rewrittenQuery != query) {
            HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(
                type,
                rewrittenQuery,
                minChildren,
                maxChildren,
                scoreMode,
                innerHitBuilder
            );
            hasChildQueryBuilder.ignoreUnmapped(ignoreUnmapped);
            hasChildQueryBuilder.scopePrefix(scopePrefix);
            return hasChildQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        if (innerHitBuilder != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : type;
            if (innerHits.containsKey(name)) {
                throw new IllegalArgumentException("[inner_hits] already contains an entry for key [" + name + "]");
            }

            Map<String, InnerHitContextBuilder> children = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(query, children);
            InnerHitContextBuilder innerHitContextBuilder = new ParentChildInnerHitContextBuilder(
                type,
                true,
                query,
                innerHitBuilder,
                children
            );
            innerHits.put(name, innerHitContextBuilder);
        }
    }

    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        query.visit(visitor.getChildVisitor(BooleanClause.Occur.MUST));
    }
}
