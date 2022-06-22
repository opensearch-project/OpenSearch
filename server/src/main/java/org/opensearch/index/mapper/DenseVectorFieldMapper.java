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

package org.opensearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Field Mapper for Dense vector type. Extends ParametrizedFieldMapper in order to easily configure mapping parameters.
 */
public class DenseVectorFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "dense_vector";

    /**
     * Define the max dimension a knn_vector mapping can have.
     */
    public static final int MAX_DIMENSION = 1024;

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper) in;
    }

    /**
     * Builder for DenseVectorFieldMapper. This class defines the set of parameters that can be applied to the knn_vector
     * field type
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, false);

        protected final Parameter<Integer> dimension = new Parameter<>(Names.DIMENSION.getValue(), false, () -> 1, (n, c, o) -> {
            int value = XContentMapValues.nodeIntegerValue(o);
            if (value > MAX_DIMENSION) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "[dimension] value cannot be greater than %d for vector [%s]", MAX_DIMENSION, name)
                );
            }
            if (value <= 0) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "[dimension] value must be greater than 0 for vector [%s]", name)
                );
            }
            return value;
        }, m -> toType(m).dimension).setSerializer((b, n, v) -> b.field(n, v.intValue()), v -> Integer.toString(v.intValue()));

        private final Parameter<KnnContext> knnContext = new Parameter<>(
            Names.KNN.getValue(),
            false,
            () -> null,
            (n, c, o) -> KnnContext.parse(o),
            m -> toType(m).knnContext
        ).setSerializer(((b, n, v) -> {
            if (v == null) {
                return;
            }
            b.startObject(n);
            v.toXContent(b, ToXContent.EMPTY_PARAMS);
            b.endObject();
        }), m -> m.getKnnAlgorithmContext().getMethod().name());

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(dimension, knnContext, hasDocValues);
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            return new DenseVectorFieldMapper(
                buildFullName(context),
                new DenseVectorFieldType(buildFullName(context), dimension.get(), knnContext.get()),
                multiFieldsBuilder.build(this, context),
                copyTo.build()
            );
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
            Object dimensionField = node.get(Names.DIMENSION.getValue());
            String dimension = XContentMapValues.nodeStringValue(dimensionField, null);
            if (dimension == null) {
                throw new MapperParsingException(String.format(Locale.ROOT, "[dimension] property must be specified for field [%s]", name));
            }
            builder.parse(name, parserContext, node);
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
        private final boolean hasDocValues;

        public DenseVectorFieldType(String name, int dimension, KnnContext knnContext) {
            this(name, Collections.emptyMap(), dimension, knnContext, false);
        }

        public DenseVectorFieldType(String name, Map<String, String> meta, int dimension, KnnContext knnContext, boolean hasDocValues) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.dimension = dimension;
            this.knnContext = knnContext;
            this.hasDocValues = hasDocValues;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Dense_vector does not support fields search");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(
                context,
                "Dense_vector does not support exact searching, use KNN queries instead [" + name() + "]"
            );
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

    public DenseVectorFieldMapper(String simpleName, DenseVectorFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
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
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new DenseVectorFieldMapper.Builder(simpleName()).init(this);
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    public DenseVectorFieldType fieldType() {
        return (DenseVectorFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
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
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
            FIELD_TYPE.freeze();
        }
    }
}
