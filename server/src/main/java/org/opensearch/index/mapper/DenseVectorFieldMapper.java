/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Field Mapper for Dense vector type. Extends ParametrizedFieldMapper in order to easily configure mapping parameters.
 *
 * @opensearch.internal
 */
public final class DenseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "dense_vector";

    /**
     * Define the max dimension a knn_vector mapping can have.
     */
    private static final int MAX_DIMENSION = 1024;

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper) in;
    }

    /**
     * Builder for DenseVectorFieldMapper. This class defines the set of parameters that can be applied to the knn_vector
     * field type
     */
    public static class Builder extends FieldMapper.Builder<Builder> {
        private CopyTo copyTo = CopyTo.empty();
        private Integer dimension = 1;
        private KnnContext knnContext = null;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            final DenseVectorFieldType mappedFieldType = new DenseVectorFieldType(buildFullName(context), dimension, knnContext);
            return new DenseVectorFieldMapper(
                buildFullName(context),
                fieldType,
                mappedFieldType,
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }

        public Builder dimension(int value) {
            if (value > MAX_DIMENSION) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "[dimension] value %d cannot be greater than %d for vector [%s]", value, MAX_DIMENSION, name)
                );
            }
            if (value <= 0) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "[dimension] value %d must be greater than 0 for vector [%s]", value, name)
                );
            }
            this.dimension = value;
            return this;
        }

        public Builder knn(KnnContext value) {
            this.knnContext = value;
            return this;
        }
    }

    /**
     * Type parser for dense_vector field mapper
     *
     * @opensearch.internal
     */
    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new DenseVectorFieldMapper.Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                switch (fieldName) {
                    case "dimension":
                        if (fieldNode == null) {
                            throw new MapperParsingException(
                                String.format(Locale.ROOT, "[dimension] property must be specified for field [%s]", name)
                            );
                        }
                        builder.dimension(XContentMapValues.nodeIntegerValue(fieldNode, 1));
                        iterator.remove();
                        break;
                    case "knn":
                        builder.knn(KnnContext.parse(fieldNode));
                        iterator.remove();
                        break;
                    default:
                        break;
                }
            }
            return builder;
        }
    }

    /**
     * Field type for dense_vector field mapper
     *
     * @opensearch.internal
     */
    public static class DenseVectorFieldType extends MappedFieldType {

        private final int dimension;
        private final KnnContext knnContext;

        public DenseVectorFieldType(String name, int dimension, KnnContext knnContext) {
            this(name, Collections.emptyMap(), dimension, knnContext, false);
        }

        public DenseVectorFieldType(String name, Map<String, String> meta, int dimension, KnnContext knnContext, boolean hasDocValues) {
            super(name, false, false, hasDocValues, TextSearchInfo.NONE, meta);
            this.dimension = dimension;
            this.knnContext = knnContext;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("[fields search] are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("[term] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            QueryShardContext context
        ) {
            throw new UnsupportedOperationException("[fuzzy] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            throw new UnsupportedOperationException("[prefix] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            QueryShardContext context
        ) {
            throw new UnsupportedOperationException("[wildcard] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            throw new UnsupportedOperationException("[regexp] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        public int getDimension() {
            return dimension;
        }

        public KnnContext getKnnContext() {
            return knnContext;
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    protected Integer dimension;
    protected boolean isKnnEnabled;
    protected KnnContext knnContext;
    protected boolean hasDocValues;
    protected String modelId;

    public DenseVectorFieldMapper(
        String simpleName,
        FieldType fieldType,
        DenseVectorFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        dimension = mappedFieldType.getDimension();
        fieldType = new FieldType(DenseVectorFieldMapper.Defaults.FIELD_TYPE);
        isKnnEnabled = mappedFieldType.getKnnContext() != null;
        if (isKnnEnabled) {
            knnContext = mappedFieldType.getKnnContext();
            fieldType.setVectorDimensionsAndSimilarityFunction(
                mappedFieldType.getDimension(),
                Metric.toSimilarityFunction(knnContext.getMetric())
            );
        }
        fieldType.freeze();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        parseCreateField(context, fieldType().getDimension());
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        DenseVectorFieldMapper denseVectorMergeWith = (DenseVectorFieldMapper) other;
        if (!Objects.equals(dimension, denseVectorMergeWith.dimension)) {
            conflicts.add("mapper [" + name() + "] has different [dimension]");
        }

        if (isOnlyOneObjectNull(knnContext, denseVectorMergeWith.knnContext)
            || (isBothObjectsNotNull(knnContext, denseVectorMergeWith.knnContext)
                && !Objects.equals(knnContext.getMetric(), denseVectorMergeWith.knnContext.getMetric()))) {
            conflicts.add("mapper [" + name() + "] has different [metric]");
        }

        if (isBothObjectsNotNull(knnContext, denseVectorMergeWith.knnContext)) {

            if (!Objects.equals(knnContext.getMetric(), denseVectorMergeWith.knnContext.getMetric())) {
                conflicts.add("mapper [" + name() + "] has different [metric]");
            }

            if (isBothObjectsNotNull(knnContext.getKnnAlgorithmContext(), denseVectorMergeWith.knnContext.getKnnAlgorithmContext())) {
                KnnAlgorithmContext knnAlgorithmContext = knnContext.getKnnAlgorithmContext();
                KnnAlgorithmContext mergeWithKnnAlgorithmContext = denseVectorMergeWith.knnContext.getKnnAlgorithmContext();

                if (isOnlyOneObjectNull(knnAlgorithmContext, mergeWithKnnAlgorithmContext)
                    || (isBothObjectsNotNull(knnAlgorithmContext, mergeWithKnnAlgorithmContext)
                        && !Objects.equals(knnAlgorithmContext.getMethod(), mergeWithKnnAlgorithmContext.getMethod()))) {
                    conflicts.add("mapper [" + name() + "] has different [method]");
                }

                if (isBothObjectsNotNull(knnAlgorithmContext, mergeWithKnnAlgorithmContext)) {
                    Map<String, Object> knnAlgoParams = knnAlgorithmContext.getParameters();
                    Map<String, Object> mergeWithKnnAlgoParams = mergeWithKnnAlgorithmContext.getParameters();

                    if (isOnlyOneObjectNull(knnAlgoParams, mergeWithKnnAlgoParams)
                        || (isBothObjectsNotNull(knnAlgoParams, mergeWithKnnAlgoParams)
                            && !Objects.equals(knnAlgoParams, mergeWithKnnAlgoParams))) {
                        conflicts.add("mapper [" + name() + "] has different [knn algorithm parameters]");
                    }
                }
            }
        }
    }

    private boolean isOnlyOneObjectNull(Object object1, Object object2) {
        return object1 == null && object2 != null || object2 == null && object1 != null;
    }

    private boolean isBothObjectsNotNull(Object object1, Object object2) {
        return object1 != null && object2 != null;
    }

    protected void parseCreateField(ParseContext context, int dimension) throws IOException {

        context.path().add(simpleName());

        ArrayList<Float> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();
        float value;
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                value = context.parser().floatValue();

                if (Float.isNaN(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be NaN");
                }

                if (Float.isInfinite(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be infinity");
                }

                vector.add(value);
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = context.parser().floatValue();

            if (Float.isNaN(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be NaN");
            }

            if (Float.isInfinite(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be infinity");
            }

            vector.add(value);
            context.parser().nextToken();
        } else if (token == XContentParser.Token.VALUE_NULL) {
            context.path().remove();
            return;
        }

        if (dimension != vector.size()) {
            String errorMessage = String.format(
                Locale.ROOT,
                "Vector dimensions mismatch, expected [%d] but given [%d]",
                dimension,
                vector.size()
            );
            throw new IllegalArgumentException(errorMessage);
        }

        float[] array = new float[vector.size()];
        int i = 0;
        for (Float f : vector) {
            array[i++] = f;
        }

        KnnVectorField point = new KnnVectorField(name(), array, fieldType);

        context.doc().add(point);
        context.path().remove();
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    public DenseVectorFieldType fieldType() {
        return (DenseVectorFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        builder.field("dimension", dimension);
        if (knnContext != null) {
            builder.startObject("knn");
            knnContext.toXContent(builder, params);
            builder.endObject();
        }
    }

    /**
     * Define names for dense_vector parameters
     *
     * @opensearch.internal
     */
    enum Names {
        DIMENSION("dimension"),
        KNN("knn");

        Names(String value) {
            this.value = value;
        }

        String value;

        String getValue() {
            return this.value;
        }
    }

    /**
     * Default parameters
     *
     * @opensearch.internal
     */
    static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
            FIELD_TYPE.freeze();
        }
    }
}
