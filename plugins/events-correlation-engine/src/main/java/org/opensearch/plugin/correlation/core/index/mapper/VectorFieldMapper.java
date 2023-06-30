/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.mapper;

import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.plugin.correlation.core.index.CorrelationParamsContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Parameterized field mapper for Correlation Vector type
 *
 * @opensearch.internal
 */
public abstract class VectorFieldMapper extends ParametrizedFieldMapper {

    /**
     * name of Correlation Vector type
     */
    public static final String CONTENT_TYPE = "correlation_vector";
    /**
     * dimension of the correlation vectors
     */
    public static final String DIMENSION = "dimension";
    /**
     * context e.g. parameters and vector similarity function of Correlation Vector type
     */
    public static final String CORRELATION_CONTEXT = "correlation_ctx";

    private static VectorFieldMapper toType(FieldMapper in) {
        return (VectorFieldMapper) in;
    }

    /**
     * definition of VectorFieldMapper.Builder
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {
        protected Boolean ignoreMalformed;

        protected final Parameter<Boolean> stored = Parameter.boolParam("store", false, m -> toType(m).stored, false);
        protected final Parameter<Boolean> hasDocValues = Parameter.boolParam("doc_values", false, m -> toType(m).hasDocValues, true);
        protected final Parameter<Integer> dimension = new Parameter<>(DIMENSION, false, () -> -1, (n, c, o) -> {
            if (o == null) {
                throw new IllegalArgumentException("Dimension cannot be null");
            }
            int value;
            try {
                value = XContentMapValues.nodeIntegerValue(o);
            } catch (Exception ex) {
                throw new IllegalArgumentException(
                    String.format(Locale.getDefault(), "Unable to parse [dimension] from provided value [%s] for vector [%s]", o, name)
                );
            }
            if (value <= 0) {
                throw new IllegalArgumentException(
                    String.format(Locale.getDefault(), "Dimension value must be greater than 0 for vector: %s", name)
                );
            }
            return value;
        }, m -> toType(m).dimension);

        protected final Parameter<CorrelationParamsContext> correlationParamsContext = new Parameter<>(
            CORRELATION_CONTEXT,
            false,
            () -> null,
            (n, c, o) -> CorrelationParamsContext.parse(o),
            m -> toType(m).correlationParams
        );

        protected final Parameter<Map<String, String>> meta = Parameter.metaParam();

        /**
         * Parameterized ctor for VectorFieldMapper.Builder
         * @param name name
         */
        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(stored, hasDocValues, dimension, meta, correlationParamsContext);
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            final CorrelationParamsContext correlationParams = correlationParamsContext.getValue();
            final MultiFields multiFieldsBuilder = this.multiFieldsBuilder.build(this, context);
            final CopyTo copyToBuilder = copyTo.build();
            final Explicit<Boolean> ignoreMalformed = ignoreMalformed(context);
            final Map<String, String> metaValue = meta.getValue();

            final CorrelationVectorFieldType mappedFieldType = new CorrelationVectorFieldType(
                buildFullName(context),
                metaValue,
                dimension.getValue(),
                correlationParams
            );

            CorrelationVectorFieldMapper.CreateLuceneFieldMapperInput createLuceneFieldMapperInput =
                new CorrelationVectorFieldMapper.CreateLuceneFieldMapperInput(
                    name,
                    mappedFieldType,
                    multiFieldsBuilder,
                    copyToBuilder,
                    ignoreMalformed,
                    stored.get(),
                    hasDocValues.get(),
                    correlationParams
                );
            return new CorrelationVectorFieldMapper(createLuceneFieldMapperInput);
        }
    }

    /**
     * deifintion of VectorFieldMapper.TypeParser
     */
    public static class TypeParser implements Mapper.TypeParser {

        /**
         * default constructor of VectorFieldMapper.TypeParser
         */
        public TypeParser() {}

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context) throws MapperParsingException {
            Builder builder = new VectorFieldMapper.Builder(name);
            builder.parse(name, context, node);

            if (builder.dimension.getValue() == -1) {
                throw new IllegalArgumentException(String.format(Locale.getDefault(), "Dimension value missing for vector: %s", name));
            }
            return builder;
        }
    }

    /**
     * deifintion of VectorFieldMapper.CorrelationVectorFieldType
     */
    public static class CorrelationVectorFieldType extends MappedFieldType {
        int dimension;
        CorrelationParamsContext correlationParams;

        /**
         * Parameterized ctor for VectorFieldMapper.CorrelationVectorFieldType
         * @param name name of the field
         * @param meta meta of the field
         * @param dimension dimension of the field
         */
        public CorrelationVectorFieldType(String name, Map<String, String> meta, int dimension) {
            this(name, meta, dimension, null);
        }

        /**
         * Parameterized ctor for VectorFieldMapper.CorrelationVectorFieldType
         * @param name name of the field
         * @param meta meta of the field
         * @param dimension dimension of the field
         * @param correlationParams correlation params for the field
         */
        public CorrelationVectorFieldType(
            String name,
            Map<String, String> meta,
            int dimension,
            CorrelationParamsContext correlationParams
        ) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
            this.dimension = dimension;
            this.correlationParams = correlationParams;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String s) {
            throw new UnsupportedOperationException("Correlation Vector do not support fields search");
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
        public Query termQuery(Object o, QueryShardContext context) {
            throw new QueryShardException(
                context,
                String.format(
                    Locale.getDefault(),
                    "Correlation vector do not support exact searching, use Correlation queries instead: [%s]",
                    name()
                )
            );
        }

        /**
         * get dimension
         * @return dimension
         */
        public int getDimension() {
            return dimension;
        }

        /**
         * get correlation params
         * @return correlation params
         */
        public CorrelationParamsContext getCorrelationParams() {
            return correlationParams;
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    protected boolean stored;
    protected boolean hasDocValues;
    protected Integer dimension;
    protected CorrelationParamsContext correlationParams;

    /**
     * Parameterized ctor for VectorFieldMapper
     * @param simpleName name of field
     * @param mappedFieldType field type of field
     * @param multiFields multi fields
     * @param copyTo copy to
     * @param ignoreMalformed ignore malformed
     * @param stored stored field
     * @param hasDocValues has doc values
     */
    public VectorFieldMapper(
        String simpleName,
        CorrelationVectorFieldType mappedFieldType,
        FieldMapper.MultiFields multiFields,
        FieldMapper.CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed,
        boolean stored,
        boolean hasDocValues
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.stored = stored;
        this.hasDocValues = hasDocValues;
        this.dimension = mappedFieldType.getDimension();
    }

    @Override
    protected VectorFieldMapper clone() {
        return (VectorFieldMapper) super.clone();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext parseContext) throws IOException {
        parseCreateField(parseContext, fieldType().getDimension());
    }

    protected abstract void parseCreateField(ParseContext parseContext, int dimension) throws IOException;

    Optional<float[]> getFloatsFromContext(ParseContext context, int dimension) throws IOException {
        context.path().add(simpleName());

        List<Float> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();
        float value;
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                value = context.parser().floatValue();

                if (Float.isNaN(value)) {
                    throw new IllegalArgumentException("Correlation vector values cannot be NaN");
                }

                if (Float.isInfinite(value)) {
                    throw new IllegalArgumentException("Correlation vector values cannot be infinity");
                }
                vector.add(value);
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = context.parser().floatValue();
            if (Float.isNaN(value)) {
                throw new IllegalArgumentException("Correlation vector values cannot be NaN");
            }

            if (Float.isInfinite(value)) {
                throw new IllegalArgumentException("Correlation vector values cannot be infinity");
            }
            vector.add(value);
            context.parser().nextToken();
        } else if (token == XContentParser.Token.VALUE_NULL) {
            context.path().remove();
            return Optional.empty();
        }

        if (dimension != vector.size()) {
            String errorMessage = String.format(
                Locale.ROOT,
                "Vector dimension mismatch. Expected: %d, Given: %d",
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
        return Optional.of(array);
    }

    @Override
    protected boolean docValuesByDefault() {
        return true;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new VectorFieldMapper.Builder(simpleName()).init(this);
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    public CorrelationVectorFieldType fieldType() {
        return (CorrelationVectorFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
    }

    /**
     * Class for constants used in parent class VectorFieldMapper
     */
    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    /**
     * Class for constants used in parent class VectorFieldMapper
     */
    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
    }
}
