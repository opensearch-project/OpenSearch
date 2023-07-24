/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.plugin.correlation.core.index.mapper.VectorFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Constructs a query to get correlated events or documents for a particular event or document.
 *
 * @opensearch.internal
 */
public class CorrelationQueryBuilder extends AbstractQueryBuilder<CorrelationQueryBuilder> {

    private static final Logger log = LogManager.getLogger(CorrelationQueryBuilder.class);
    protected static final ParseField VECTOR_FIELD = new ParseField("vector");
    protected static final ParseField K_FIELD = new ParseField("k");
    protected static final ParseField FILTER_FIELD = new ParseField("filter");
    /**
     * max number of neighbors that can be retrieved.
     */
    public static int K_MAX = 10000;

    /**
     * name of the query
     */
    public static final ParseField NAME_FIELD = new ParseField("correlation");

    private String fieldName;
    private float[] vector;
    private int k = 0;
    private double boost;
    private QueryBuilder filter;

    private CorrelationQueryBuilder() {}

    /**
     * parameterized ctor for CorrelationQueryBuilder
     * @param fieldName field name for query
     * @param vector query vector
     * @param k number of nearby neighbors
     */
    public CorrelationQueryBuilder(String fieldName, float[] vector, int k) {
        this(fieldName, vector, k, null);
    }

    /**
     * parameterized ctor for CorrelationQueryBuilder
     * @param fieldName field name for query
     * @param vector query vector
     * @param k number of nearby neighbors
     * @param filter optional filter query
     */
    public CorrelationQueryBuilder(String fieldName, float[] vector, int k, QueryBuilder filter) {
        if (Strings.isNullOrEmpty(fieldName)) {
            throw new IllegalArgumentException(
                String.format(Locale.getDefault(), "[%s] requires fieldName", NAME_FIELD.getPreferredName())
            );
        }
        if (vector == null) {
            throw new IllegalArgumentException(
                String.format(Locale.getDefault(), "[%s] requires query vector", NAME_FIELD.getPreferredName())
            );
        }
        if (vector.length == 0) {
            throw new IllegalArgumentException(
                String.format(Locale.getDefault(), "[%s] query vector is empty", NAME_FIELD.getPreferredName())
            );
        }
        if (k <= 0) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), "[%s] requires k > 0", NAME_FIELD.getPreferredName()));
        }
        if (k > K_MAX) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), "[%s] requires k <= ", K_MAX));
        }

        this.fieldName = fieldName;
        this.vector = vector;
        this.k = k;
        this.filter = filter;
    }

    /**
     * parameterized ctor for CorrelationQueryBuilder
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public CorrelationQueryBuilder(StreamInput sin) throws IOException {
        super(sin);
        this.fieldName = sin.readString();
        this.vector = sin.readFloatArray();
        this.k = sin.readInt();
        this.filter = sin.readOptionalNamedWriteable(QueryBuilder.class);
    }

    private static float[] objectsToFloats(List<Object> objs) {
        float[] vector = new float[objs.size()];
        for (int i = 0; i < objs.size(); ++i) {
            vector[i] = ((Number) objs.get(i)).floatValue();
        }
        return vector;
    }

    /**
     * parse into CorrelationQueryBuilder
     * @param xcp XContentParser
     * @return CorrelationQueryBuilder
     */
    public static CorrelationQueryBuilder parse(XContentParser xcp) throws IOException {
        String fieldName = null;
        List<Object> vector = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        int k = 0;
        QueryBuilder filter = null;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = xcp.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = xcp.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME_FIELD.getPreferredName(), xcp.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = xcp.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = xcp.currentName();
                    } else if (token.isValue() || token == XContentParser.Token.START_ARRAY) {
                        if (VECTOR_FIELD.match(currentFieldName, xcp.getDeprecationHandler())) {
                            vector = xcp.list();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, xcp.getDeprecationHandler())) {
                            boost = xcp.floatValue();
                        } else if (K_FIELD.match(currentFieldName, xcp.getDeprecationHandler())) {
                            k = (Integer) NumberFieldMapper.NumberType.INTEGER.parse(xcp.objectBytes(), false);
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, xcp.getDeprecationHandler())) {
                            queryName = xcp.text();
                        } else {
                            throw new ParsingException(
                                xcp.getTokenLocation(),
                                "[" + NAME_FIELD.getPreferredName() + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        String tokenName = xcp.currentName();
                        if (FILTER_FIELD.getPreferredName().equals(tokenName)) {
                            filter = parseInnerQueryBuilder(xcp);
                        } else {
                            throw new ParsingException(
                                xcp.getTokenLocation(),
                                "[" + NAME_FIELD.getPreferredName() + "] unknown token [" + token + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            xcp.getTokenLocation(),
                            "[" + NAME_FIELD.getPreferredName() + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME_FIELD.getPreferredName(), xcp.getTokenLocation(), fieldName, xcp.currentName());
                fieldName = xcp.currentName();
                vector = xcp.list();
            }
        }

        assert vector != null;
        CorrelationQueryBuilder correlationQueryBuilder = new CorrelationQueryBuilder(fieldName, objectsToFloats(vector), k, filter);
        correlationQueryBuilder.queryName(queryName);
        correlationQueryBuilder.boost(boost);
        return correlationQueryBuilder;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * get field name
     * @return field name
     */
    public String fieldName() {
        return fieldName;
    }

    public void setVector(float[] vector) {
        this.vector = vector;
    }

    /**
     * get query vector
     * @return query vector
     */
    public Object vector() {
        return vector;
    }

    public void setK(int k) {
        this.k = k;
    }

    /**
     * get number of nearby neighbors
     * @return number of nearby neighbors
     */
    public int getK() {
        return k;
    }

    public void setBoost(double boost) {
        this.boost = boost;
    }

    /**
     * get boost
     * @return boost
     */
    public double getBoost() {
        return boost;
    }

    public void setFilter(QueryBuilder filter) {
        this.filter = filter;
    }

    /**
     * get optional filter
     * @return optional filter
     */
    public QueryBuilder getFilter() {
        return filter;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeFloatArray(vector);
        out.writeInt(k);
        out.writeOptionalNamedWriteable(filter);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(fieldName);

        builder.field(VECTOR_FIELD.getPreferredName(), vector);
        builder.field(K_FIELD.getPreferredName(), k);
        if (filter != null) {
            builder.field(FILTER_FIELD.getPreferredName(), filter);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType mappedFieldType = context.fieldMapper(fieldName);

        if (!(mappedFieldType instanceof VectorFieldMapper.CorrelationVectorFieldType)) {
            throw new IllegalArgumentException(String.format(Locale.getDefault(), "Field '%s' is not knn_vector type.", this.fieldName));
        }

        VectorFieldMapper.CorrelationVectorFieldType correlationVectorFieldType =
            (VectorFieldMapper.CorrelationVectorFieldType) mappedFieldType;
        int fieldDimension = correlationVectorFieldType.getDimension();

        if (fieldDimension != vector.length) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.getDefault(),
                    "Query vector has invalid dimension: %d. Dimension should be: %d",
                    vector.length,
                    fieldDimension
                )
            );
        }

        String indexName = context.index().getName();
        CorrelationQueryFactory.CreateQueryRequest createQueryRequest = new CorrelationQueryFactory.CreateQueryRequest(
            indexName,
            this.fieldName,
            this.vector,
            this.k,
            this.filter,
            context
        );
        return CorrelationQueryFactory.create(createQueryRequest);
    }

    @Override
    protected boolean doEquals(CorrelationQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Arrays.equals(vector, other.vector) && Objects.equals(k, other.k);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, vector, k);
    }

    @Override
    public String getWriteableName() {
        return NAME_FIELD.getPreferredName();
    }
}
