/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.internal.HybridQueryContext;
import org.opensearch.search.query.HybridQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Class abstract creation of a Query type "hybrid". Hybrid query will allow execution of multiple sub-queries and
 * collects score for each of those sub-query.
 */
public final class HybridQueryBuilder extends AbstractQueryBuilder<HybridQueryBuilder> {
    private static final Logger logger = LogManager.getLogger(HybridQueryBuilder.class);
    public static final String NAME = "hybrid";

    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    private static final ParseField FILTER_FIELD = new ParseField("filter");
    private static final ParseField PAGINATION_DEPTH_FIELD = new ParseField("pagination_depth");

    private final List<QueryBuilder> queries = new ArrayList<>();

    private Integer paginationDepth;

    public static final int MAX_NUMBER_OF_SUB_QUERIES = 5;
    private static final int LOWER_BOUND_OF_PAGINATION_DEPTH = 0;

    public HybridQueryBuilder() {}

    public HybridQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queries.addAll(readQueries(in));
        paginationDepth = in.readOptionalInt();
    }

    /**
     * Serialize this query object into input stream
     * @param out stream that we'll be used for serialization
     * @throws IOException
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queries);
        out.writeOptionalInt(paginationDepth);
    }

    /**
     * Add one sub-query
     * @param queryBuilder
     * @return
     */
    public HybridQueryBuilder add(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "inner %s query clause cannot be null", NAME));
        }
        queries.add(queryBuilder);
        return this;
    }

    /**
     * Function to support filter on HybridQueryBuilder filter.
     * If the filter is null, then we do nothing and return.
     * Otherwise, we push down the filter to queries list.
     * @param filter the filter parameter
     * @return HybridQueryBuilder itself
     */
    public QueryBuilder filter(QueryBuilder filter) {
        if (validateFilterParams(filter) == false) {
            return this;
        }
        ListIterator<QueryBuilder> iterator = queries.listIterator();
        while (iterator.hasNext()) {
            QueryBuilder query = iterator.next();
            // set the query again because query.filter(filter) can return new query.
            iterator.set(query.filter(filter));
        }
        return this;
    }

    /**
     * Create builder object with a content of this hybrid query
     * @param builder
     * @param params
     * @throws IOException
     */
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        // TODO https://github.com/opensearch-project/neural-search/issues/1097
        if (Objects.nonNull(paginationDepth)) {
            builder.field(PAGINATION_DEPTH_FIELD.getPreferredName(), paginationDepth);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /**
     * Create query object for current hybrid query using shard context
     * @param queryShardContext context object that used to create hybrid query
     * @return hybrid query object
     * @throws IOException
     */
    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) throws IOException {
        Collection<Query> queryCollection = toQueries(queries, queryShardContext);
        if (queryCollection.isEmpty()) {
            return Queries.newMatchNoDocsQuery(String.format(Locale.ROOT, "no clauses for %s query", NAME));
        }
        validatePaginationDepth(paginationDepth, queryShardContext);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(paginationDepth).build();
        return new HybridQuery(queryCollection, hybridQueryContext);
    }

    /**
     * Creates HybridQueryBuilder from xContent.
     * Example of a json for Hybrid Query:
     * {
     *      "query": {
     *          "hybrid": {
     *              "queries": [
     *                  {
     *                      "neural": {
     *                          "text_knn": {
     *                                    "query_text": "Hello world",
     *                                    "model_id": "dcsdcasd",
     *                                    "k": 10
     *                                }
     *                            }
     *                   },
     *                   {
     *                      "term": {
     *                          "text": "keyword"
     *                       }
     *                    }
     *               ]
     *               "filter":
     *                  "term": {
     *                      "text": "keyword"
     *                  }
     *          }
     *     }
     * }
     *
     * @param parser parser that has been initialized with the query content
     * @return new instance of HybridQueryBuilder
     * @throws IOException
     */
    public static HybridQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        Integer paginationDepth = null;
        final List<QueryBuilder> queries = new ArrayList<>();
        QueryBuilder filter = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queries.add(parseInnerQueryBuilder(parser));
                } else if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    filter = parseInnerQueryBuilder(parser);
                } else {
                    logger.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, currentFieldName));
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME)
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while (token != XContentParser.Token.END_ARRAY) {
                        if (queries.size() == MAX_NUMBER_OF_SUB_QUERIES) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                String.format(Locale.ROOT, "Number of sub-queries exceeds maximum supported by [%s] query", NAME)
                            );
                        }
                        queries.add(parseInnerQueryBuilder(parser));
                        token = parser.nextToken();
                    }
                } else {
                    logger.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, currentFieldName));
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME)
                    );
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                    // regular boost functionality is not supported, user should use score normalization methods to manipulate with scores
                    if (boost != DEFAULT_BOOST) {
                        logger.error("[{}] query does not support provided value {} for [{}]", NAME, boost, BOOST_FIELD);
                        throw new ParsingException(parser.getTokenLocation(), "[{}] query does not support [{}]", NAME, BOOST_FIELD);
                    }
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (PAGINATION_DEPTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    paginationDepth = parser.intValue();
                } else {
                    logger.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, currentFieldName));
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME)
                    );
                }
            }
        }

        if (queries.isEmpty()) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "[%s] requires 'queries' field with at least one clause", NAME)
            );
        }

        HybridQueryBuilder compoundQueryBuilder = new HybridQueryBuilder();
        compoundQueryBuilder.queryName(queryName);
        compoundQueryBuilder.boost(boost);
        compoundQueryBuilder.setPaginationDepth(paginationDepth);
        for (QueryBuilder query : queries) {
            if (filter == null) {
                compoundQueryBuilder.add(query);
            } else {
                compoundQueryBuilder.add(query.filter(filter));
            }
        }
        return compoundQueryBuilder;
    }

    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        HybridQueryBuilder newBuilder = new HybridQueryBuilder();
        boolean changed = false;
        for (QueryBuilder query : queries) {
            QueryBuilder result = query.rewrite(queryShardContext);
            if (result != query) {
                changed = true;
            }
            newBuilder.add(result);
        }
        if (changed) {
            newBuilder.queryName(queryName);
            newBuilder.boost(boost);
            newBuilder.setPaginationDepth(paginationDepth);
            return newBuilder;
        } else {
            return this;
        }
    }

    /**
     * Indicates whether some other QueryBuilder object of the same type is "equal to" this one.
     * @param obj
     * @return true if objects are equal
     */
    @Override
    protected boolean doEquals(HybridQueryBuilder obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(queries, obj.queries);
        equalsBuilder.append(paginationDepth, obj.paginationDepth);
        return equalsBuilder.isEquals();
    }

    /**
     * Create hash code for current hybrid query builder object
     * @return hash code
     */
    @Override
    protected int doHashCode() {
        return Objects.hash(queries, paginationDepth);
    }

    /**
     * Returns the name of the writeable object
     * @return
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static void validatePaginationDepth(final Integer paginationDepth, final QueryShardContext queryShardContext) {
        if (Objects.isNull(paginationDepth)) {
            return;
        }
        if (paginationDepth < LOWER_BOUND_OF_PAGINATION_DEPTH) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "pagination_depth should be greater than %s", LOWER_BOUND_OF_PAGINATION_DEPTH)
            );
        }
        // compare pagination depth with OpenSearch setting index.max_result_window
        // see https://opensearch.org/docs/latest/install-and-configure/configuring-opensearch/index-settings/
        int maxResultWindowIndexSetting = queryShardContext.getIndexSettings().getMaxResultWindow();
        if (paginationDepth > maxResultWindowIndexSetting) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "pagination_depth should be less than or equal to %s setting",
                    IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                )
            );
        }
    }

    /**
     * visit method to parse the HybridQueryBuilder by a visitor
     */
    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        // getChildVisitor of NeuralSearchQueryVisitor return this.
        // therefore any argument can be passed. Here we have used Occcur.MUST as an argument.
        QueryBuilderVisitor subVisitor = visitor.getChildVisitor(Occur.MUST);
        for (QueryBuilder subQueryBuilder : queries) {
            subQueryBuilder.visit(subVisitor);
        }
    }

    /**
     * Extracts the inner hits from the hybrid query tree structure.
     * While it extracts inner hits, child inner hits are inlined into the inner hit builder they belong to.
     * This implementation handles inner hits for all sub-queries within the hybrid query.
     *
     * @param innerHits the map to collect inner hit contexts, where the key is the inner hit name
     *                   and the value is the corresponding inner hit context builder
     */
    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        for (QueryBuilder queryBuilder : queries) {
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHits);
        }
    }

    public List<QueryBuilder> getQueries() {
        return queries;
    }

    public Integer getPaginationDepth() {
        return paginationDepth;
    }

    public void setPaginationDepth(Integer paginationDepth) {
        this.paginationDepth = paginationDepth;
    }
}
