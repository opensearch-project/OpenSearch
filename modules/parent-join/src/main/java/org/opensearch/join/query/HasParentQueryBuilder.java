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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
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
 * Builder for the 'has_parent' query.
 */
public class HasParentQueryBuilder extends AbstractQueryBuilder<HasParentQueryBuilder> {
    public static final String NAME = "has_parent";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField PARENT_TYPE_FIELD = new ParseField("parent_type");
    private static final ParseField SCORE_FIELD = new ParseField("score");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");
    private static final ParseField SCOPE_PREFIX_FIELD = new ParseField("scope_prefix");

    private final QueryBuilder query;
    private final String type;
    private final boolean score;
    private InnerHitBuilder innerHitBuilder;
    private boolean ignoreUnmapped = false;

    /**
     * Optional join-field term prefix used to build a group-scoped global ordinals map, avoiding the shard-wide
     * global-ordinals rewrite. This is an optimisation for indices that store documents from many groups in the same
     * index, whose document ids are prefixed with a group id, and that issue group-scoped {@code has_parent} queries.
     * See {@link org.opensearch.index.fielddata.ordinals.GlobalOrdinalsBuilder#buildScoped}.
     */
    private String scopePrefix = null;

    public HasParentQueryBuilder(String type, QueryBuilder query, boolean score) {
        this(type, query, score, null);
    }

    private HasParentQueryBuilder(String type, QueryBuilder query, boolean score, InnerHitBuilder innerHitBuilder) {
        this.type = requireValue(type, "[" + NAME + "] requires '" + PARENT_TYPE_FIELD.getPreferredName() + "' field");
        this.query = requireValue(query, "[" + NAME + "] requires '" + QUERY_FIELD.getPreferredName() + "' field");
        this.score = score;
        this.innerHitBuilder = innerHitBuilder;
    }

    /**
     * Read from a stream.
     */
    public HasParentQueryBuilder(StreamInput in) throws IOException {
        super(in);
        type = in.readString();
        score = in.readBoolean();
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
        out.writeBoolean(score);
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeBoolean(ignoreUnmapped);
        if (out.getVersion().onOrAfter(Version.V_3_10_0)) {
            out.writeOptionalString(scopePrefix);
        } else if (scopePrefix != null) {
            throw new IllegalArgumentException(
                "[has_parent] scope_prefix is not supported on nodes with version [" + out.getVersion() + "], which is before 3.10.0"
            );
        }
    }

    /**
     * Returns the query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Sets an optional join-field term prefix (e.g. {@code "<groupId>:"}) used to build a group-scoped global
     * ordinals map instead of the shard-wide one. The caller must ensure the query is also scoped to the same
     * group, otherwise matches outside the prefix will be missed.
     */
    public HasParentQueryBuilder scopePrefix(String scopePrefix) {
        this.scopePrefix = scopePrefix;
        return this;
    }

    public String scopePrefix() {
        return scopePrefix;
    }

    /**
     * Returns <code>true</code> if the parent score is mapped into the child documents
     */
    public boolean score() {
        return score;
    }

    /**
     * Returns the parents type name
     */
    public String type() {
        return type;
    }

    /**
     *  Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    public HasParentQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHitBuilder = innerHit;
        innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        return this;
    }

    /**
     * Sets whether the query builder should ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public HasParentQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
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

        ParentIdFieldMapper parentIdFieldMapper = joinFieldMapper.getParentIdFieldMapper(type, true);
        if (parentIdFieldMapper != null) {
            Query parentFilter = parentIdFieldMapper.getParentFilter();
            Query innerQuery = Queries.filtered(query.toQuery(context), parentFilter);
            Query childFilter = parentIdFieldMapper.getChildrenFilter();
            MappedFieldType fieldType = parentIdFieldMapper.fieldType();
            if (scopePrefix != null) {
                // With scope_prefix the parent-join OrdinalMap is built over ONLY the join terms starting with
                // the prefix. JoinUtil.createJoinQuery(joinField, from=innerQuery, to=childFilter, ...) both
                // (a) collects the "from" (parent) side into a bitset of found ordinals AND (b) has the
                // resulting GlobalOrdinalsQuery iterate the "to" (child) side, reading each doc's join ordinal
                // via that scoped map and doing foundOrds.get(ord). Any doc on EITHER side whose join key is
                // outside the prefix resolves to a negative ordinal (not in the scoped map), which then blows
                // up as a negative array index (LongBitSet.get(-N)). So restrict BOTH sides to the prefix.
                //
                // The join field value is "<groupId>:<parentId>" for a parent doc and the same parent id for
                // its children, so a single PrefixQuery on the join field is exactly the per-group filter for
                // both sides.
                Query joinPrefixFilter = new PrefixQuery(new Term(fieldType.name(), new BytesRef(scopePrefix)));
                innerQuery = new BooleanQuery.Builder().add(innerQuery, BooleanClause.Occur.MUST)
                    .add(joinPrefixFilter, BooleanClause.Occur.FILTER)
                    .build();
                childFilter = new BooleanQuery.Builder().add(childFilter, BooleanClause.Occur.MUST)
                    .add(joinPrefixFilter, BooleanClause.Occur.FILTER)
                    .build();
            }
            final SortedSetOrdinalsIndexFieldData fieldData = context.getForField(fieldType);
            return new HasChildQueryBuilder.LateParsingQuery(
                childFilter,
                innerQuery,
                HasChildQueryBuilder.DEFAULT_MIN_CHILDREN,
                HasChildQueryBuilder.DEFAULT_MAX_CHILDREN,
                fieldType.name(),
                score ? ScoreMode.Max : ScoreMode.None,
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
                    "[" + NAME + "] join field [" + joinFieldMapper.name() + "] doesn't hold [" + type + "] as a parent"
                );
            }
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(PARENT_TYPE_FIELD.getPreferredName(), type);
        builder.field(SCORE_FIELD.getPreferredName(), score);
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

    public static HasParentQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String parentType = null;
        boolean score = false;
        String queryName = null;
        InnerHitBuilder innerHits = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        String scopePrefix = null;

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
                    innerHits = InnerHitBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (PARENT_TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    parentType = parser.text();
                } else if (SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    score = parser.booleanValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (SCOPE_PREFIX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scopePrefix = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            }
        }
        HasParentQueryBuilder queryBuilder = new HasParentQueryBuilder(parentType, iqb, score).ignoreUnmapped(ignoreUnmapped)
            .queryName(queryName)
            .boost(boost);
        if (innerHits != null) {
            queryBuilder.innerHit(innerHits);
        }
        queryBuilder.scopePrefix(scopePrefix);
        return queryBuilder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(HasParentQueryBuilder that) {
        return Objects.equals(query, that.query)
            && Objects.equals(type, that.type)
            && Objects.equals(score, that.score)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(ignoreUnmapped, that.ignoreUnmapped)
            && Objects.equals(scopePrefix, that.scopePrefix);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, score, innerHitBuilder, ignoreUnmapped, scopePrefix);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryShardContext);
        if (rewrittenQuery != query) {
            HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(type, rewrittenQuery, score, innerHitBuilder);
            hasParentQueryBuilder.ignoreUnmapped(ignoreUnmapped);
            hasParentQueryBuilder.scopePrefix(scopePrefix);
            return hasParentQueryBuilder;
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
                false,
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
