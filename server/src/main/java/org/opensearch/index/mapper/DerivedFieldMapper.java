/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.opensearch.common.Nullable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.Script;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A field mapper for derived fields
 *
 * @opensearch.internal
 */
public class DerivedFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "derived_field";

    /**
     * Default parameters for the boolean field mapper
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    private static DerivedFieldMapper toType(FieldMapper in) {
        return (DerivedFieldMapper) in;
    }

    /**
     * Builder for this field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        // TODO: The type of parameter may change here if the actual underlying FieldType object is needed
        private final Parameter<String> type = Parameter.stringParam(
            "type",
            false,
            m -> toType(m).type,
            "text"
        );

        private final Parameter<Script> script = new Parameter<>(
            "script",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : Script.parse(o),
            m -> toType(m).script
        ).setSerializerCheck((id, ic, value) -> value != null);

        public Builder(String name) { super(name); }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(type, script);
        }

        @Override
        public DerivedFieldMapper build(BuilderContext context) {
            MappedFieldType ft = new DerivedFieldType(buildFullName(context));
            return new DerivedFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * Field type for derived field mapper
     *
     * @opensearch.internal
     */
    public static final class DerivedFieldType extends MappedFieldType {

        public DerivedFieldType(
            String name,
            boolean isIndexed,
            boolean isStored,
            boolean hasDocValues,
            Map<String, String> meta
        ) {
            super(name, isIndexed, isStored, hasDocValues, TextSearchInfo.NONE, meta);
        }

        public DerivedFieldType(String name) { this(name, false, false, false, Collections.emptyMap()); }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format ) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            // TODO Return to this during Query implementation. The derived fields don't typically exist in _source but
            //  we may want fetch the field name from source if a 'script' is not provided.
            return new SourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return null;
                }
            };
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support term queries");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support exist queries");
        }

        @Override
        public boolean isAggregatable() { return false; }
    }

    private final String type;

    private final Script script;

    protected DerivedFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.type = builder.type.getValue();
        this.script = builder.script.getValue();
    }

    @Override
    public DerivedFieldType fieldType() { return (DerivedFieldType) super.fieldType(); }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // Leaving this empty as the parsing should be handled via the Builder when root object is parsed.
        // The context would not contain anything in this case since the DerivedFieldMapper is not indexed or stored.
        throw new UnsupportedOperationException("should not be invoked");
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() { return new Builder(simpleName()).init(this); }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        getMergeBuilder().toXContent(builder, includeDefaults);
        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);
    }

    public String getType() { return type; }

    public Script getScript() { return script; }
}
