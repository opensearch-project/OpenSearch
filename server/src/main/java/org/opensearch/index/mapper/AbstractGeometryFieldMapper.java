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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.geo.GeoJsonGeometryFormat;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.MapXContentParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Base field mapper class for all spatial field types
 *
 * @opensearch.internal
 */
public abstract class AbstractGeometryFieldMapper<Parsed, Processed> extends FieldMapper {

    /**
     * String parameter names for the base geometry field mapper
     *
     * @opensearch.internal
     */
    public static class Names {
        public static final ParseField IGNORE_MALFORMED = new ParseField("ignore_malformed");
        public static final ParseField IGNORE_Z_VALUE = new ParseField("ignore_z_value");
    }

    /**
     * Default parameters for the base geometry field mapper
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_Z_VALUE = new Explicit<>(true, false);
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * Interface representing an preprocessor in geometry indexing pipeline
     *
     * @opensearch.internal
     */
    public interface Indexer<Parsed, Processed> {
        Processed prepareForIndexing(Parsed geometry);

        Class<Processed> processedClass();

        List<IndexableField> indexShape(ParseContext context, Processed shape);
    }

    /**
     * Interface representing parser in geometry indexing pipeline.
     *
     * @opensearch.internal
     */
    public abstract static class Parser<Parsed> {
        /**
         * Parse the given xContent value to an object of type {@link Parsed}. The value can be
         * in any supported format.
         */
        public abstract Parsed parse(XContentParser parser) throws IOException, ParseException;

        /**
         * Given a parsed value and a format string, formats the value into a plain Java object.
         * <p>
         * Supported formats include 'geojson' and 'wkt'. The different formats are defined
         * as subclasses of {@link org.opensearch.common.geo.GeometryFormat}.
         */
        public abstract Object format(Parsed value, String format);

        /**
         * Parses the given value, then formats it according to the 'format' string.
         * <p>
         * By default, this method simply parses the value using {@link Parser#parse}, then formats
         * it with {@link Parser#format}. However some {@link Parser} implementations override this
         * as they can avoid parsing the value if it is already in the right format.
         */
        public Object parseAndFormatObject(Object value, String format) {
            Parsed geometry;
            try (
                XContentParser parser = new MapXContentParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    Collections.singletonMap("dummy_field", value),
                    MediaTypeRegistry.JSON
                )
            ) {
                parser.nextToken(); // start object
                parser.nextToken(); // field name
                parser.nextToken(); // field value
                geometry = parse(parser);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            return format(geometry, format);
        }
    }

    /**
     * Builder for the base geometry field mapper
     *
     * @opensearch.internal
     */
    public abstract static class Builder<T extends Builder<T, FT>, FT extends AbstractGeometryFieldType> extends FieldMapper.Builder<T> {
        protected Boolean ignoreMalformed;
        protected Boolean ignoreZValue;
        protected boolean indexed = true;

        public Builder(String name, FieldType fieldType) {
            super(name, fieldType);
        }

        public Builder(String name, FieldType fieldType, boolean ignoreMalformed, boolean ignoreZValue) {
            super(name, fieldType);
            this.ignoreMalformed = ignoreMalformed;
            this.ignoreZValue = ignoreZValue;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return this;
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

        public Explicit<Boolean> ignoreMalformed() {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        protected Explicit<Boolean> ignoreZValue(BuilderContext context) {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return Defaults.IGNORE_Z_VALUE;
        }

        public Explicit<Boolean> ignoreZValue() {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return Defaults.IGNORE_Z_VALUE;
        }

        public Builder ignoreZValue(final boolean ignoreZValue) {
            this.ignoreZValue = ignoreZValue;
            return this;
        }
    }

    /**
     * Base type parser for geometry field mappers
     *
     * @opensearch.internal
     */
    public abstract static class TypeParser<T extends Builder> implements Mapper.TypeParser {
        protected abstract T newBuilder(String name, Map<String, Object> params);

        public T parse(String name, Map<String, Object> node, Map<String, Object> params, ParserContext parserContext) {
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (Names.IGNORE_MALFORMED.match(propName, LoggingDeprecationHandler.INSTANCE)) {
                    params.put(
                        Names.IGNORE_MALFORMED.getPreferredName(),
                        XContentMapValues.nodeBooleanValue(propNode, name + ".ignore_malformed")
                    );
                    iterator.remove();
                } else if (Names.IGNORE_Z_VALUE.getPreferredName().equals(propName)) {
                    params.put(
                        GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(),
                        XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_Z_VALUE.getPreferredName())
                    );
                    iterator.remove();
                }
            }

            T builder = newBuilder(name, params);

            if (params.containsKey(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName())) {
                builder.ignoreZValue((Boolean) params.get(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName()));
            }

            if (params.containsKey(Names.IGNORE_MALFORMED.getPreferredName())) {
                builder.ignoreMalformed((Boolean) params.get(Names.IGNORE_MALFORMED.getPreferredName()));
            }
            return builder;
        }

        /**
         * Parse the node with the field name as name; using various parse methods for different attributes.
         */
        @Override
        public T parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            final T builder = parse(name, node, new HashMap<>(), parserContext);
            // parse the common attributes(like doc_values, boosts etc.) and set them in the builder.
            TypeParsers.parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    /**
     * Base field type for all geometry fields
     *
     * @opensearch.internal
     */
    public abstract static class AbstractGeometryFieldType<Parsed, Processed> extends MappedFieldType {

        protected Indexer<Parsed, Processed> geometryIndexer;
        protected Parser<Parsed> geometryParser;
        protected final boolean parsesArrayValue;

        protected AbstractGeometryFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            boolean parsesArrayValue,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.parsesArrayValue = parsesArrayValue;
        }

        public void setGeometryIndexer(Indexer<Parsed, Processed> geometryIndexer) {
            this.geometryIndexer = geometryIndexer;
        }

        protected Indexer<Parsed, Processed> geometryIndexer() {
            return geometryIndexer;
        }

        public void setGeometryParser(Parser<Parsed> geometryParser) {
            this.geometryParser = geometryParser;
        }

        protected Parser<Parsed> geometryParser() {
            return geometryParser;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(
                context,
                "Geometry fields do not support exact searching, use dedicated geometry queries instead: [" + name() + "]"
            );
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            String geoFormat = format != null ? format : GeoJsonGeometryFormat.NAME;

            Function<Object, Object> valueParser = value -> geometryParser.parseAndFormatObject(value, geoFormat);
            if (parsesArrayValue) {
                return new ArraySourceValueFetcher(name(), context) {
                    @Override
                    protected Object parseSourceValue(Object value) {
                        return valueParser.apply(value);
                    }
                };
            } else {
                return new SourceValueFetcher(name(), context) {
                    @Override
                    protected Object parseSourceValue(Object value) {
                        return valueParser.apply(value);
                    }
                };
            }
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    protected Explicit<Boolean> ignoreZValue;

    protected AbstractGeometryFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> ignoreZValue,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        AbstractGeometryFieldMapper gsfm = (AbstractGeometryFieldMapper) other;

        if (gsfm.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gsfm.ignoreMalformed;
        }
        if (gsfm.ignoreZValue.explicit()) {
            this.ignoreZValue = gsfm.ignoreZValue;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public AbstractGeometryFieldType<Parsed, Processed> fieldType() {
        return (AbstractGeometryFieldType<Parsed, Processed>) mappedFieldType;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    protected abstract void addStoredFields(ParseContext context, Processed geometry);

    protected abstract void addDocValuesFields(String name, Processed geometry, List<IndexableField> fields, ParseContext context);

    protected abstract void addMultiFields(ParseContext context, Processed geometry) throws IOException;

    /** parsing logic for geometry indexing */
    @Override
    public void parse(ParseContext context) throws IOException {
        AbstractGeometryFieldType<Parsed, Processed> mappedFieldType = fieldType();

        Indexer<Parsed, Processed> geometryIndexer = mappedFieldType.geometryIndexer();
        Parser<Parsed> geometryParser = mappedFieldType.geometryParser();
        try {
            Processed shape = context.parseExternalValue(geometryIndexer.processedClass());
            if (shape == null) {
                Parsed geometry = geometryParser.parse(context.parser());
                if (geometry == null) {
                    return;
                }
                shape = geometryIndexer.prepareForIndexing(geometry);
            }

            List<IndexableField> fields = new ArrayList<>();
            if (mappedFieldType.isSearchable() || mappedFieldType.hasDocValues()) {
                fields.addAll(geometryIndexer.indexShape(context, shape));
            }

            // indexed:
            List<IndexableField> indexedFields = new ArrayList<>();
            if (mappedFieldType.isSearchable()) {
                indexedFields.addAll(fields);
            }
            // stored:
            if (fieldType.stored()) {
                addStoredFields(context, shape);
            }
            // docValues:
            if (fieldType().hasDocValues()) {
                addDocValuesFields(mappedFieldType.name(), shape, fields, context);
            } else if (fieldType.stored() || fieldType().isSearchable()) {
                createFieldNamesField(context);
            }

            // add the indexed fields to the doc:
            for (IndexableField field : indexedFields) {
                context.doc().add(field);
            }

            // add multifields (e.g., used for completion suggester)
            addMultiFields(context, shape);
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(), fieldType().typeName());
            }
            context.addIgnoredField(mappedFieldType.name());
        }
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED.getPreferredName(), ignoreMalformed.value());
        }
        if (includeDefaults || ignoreZValue.explicit()) {
            builder.field(Names.IGNORE_Z_VALUE.getPreferredName(), ignoreZValue.value());
        }
    }

    @Override
    protected Explicit<Boolean> ignoreMalformed() {
        return ignoreMalformed;
    }

    public Explicit<Boolean> ignoreZValue() {
        return ignoreZValue;
    }
}
