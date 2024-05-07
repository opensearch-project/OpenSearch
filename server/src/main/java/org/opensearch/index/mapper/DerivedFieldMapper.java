/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * A field mapper for derived fields
 *
 * @opensearch.internal
 */
public class DerivedFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "derived";

    protected final IndexAnalyzers indexAnalyzers;

    private static DerivedFieldMapper toType(FieldMapper in) {
        return (DerivedFieldMapper) in;
    }

    /**
     * Builder for this field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final Parameter<String> type = Parameter.stringParam("type", false, m -> toType(m).type, "");

        private final IndexAnalyzers indexAnalyzers;

        private final Parameter<Script> script = new Parameter<>(
            "script",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : Script.parse(o),
            m -> toType(m).script
        ).setSerializerCheck((id, ic, value) -> value != null);

        private final Parameter<String> sourceIndexedField = Parameter.stringParam(
            "source_indexed_field",
            true,
            m -> toType(m).sourceIndexedField,
            ""
        );

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexAnalyzers = indexAnalyzers;
        }

        public Builder(DerivedField derivedField, IndexAnalyzers indexAnalyzers) {
            super(derivedField.getName());
            this.type.setValue(derivedField.getType());
            this.script.setValue(derivedField.getScript());
            this.sourceIndexedField.setValue(derivedField.getSourceIndexedField());
            this.indexAnalyzers = indexAnalyzers;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(type, script, sourceIndexedField);
        }

        @Override
        public DerivedFieldMapper build(BuilderContext context) {
            FieldMapper fieldMapper = DerivedFieldSupportedTypes.getFieldMapperFromType(type.getValue(), name, context, indexAnalyzers);
            Function<Object, IndexableField> fieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
                type.getValue(),
                name
            );
            DerivedFieldType ft;
            if (name.contains(".")) {
                ft = new ObjectDerivedFieldType(
                    new DerivedField(buildFullName(context), type.getValue(), script.getValue(), sourceIndexedField.getValue()),
                    fieldMapper,
                    fieldFunction,
                    indexAnalyzers
                );
            } else {
                ft = new DerivedFieldType(
                    new DerivedField(buildFullName(context), type.getValue(), script.getValue(), sourceIndexedField.getValue()),
                    fieldMapper,
                    fieldFunction,
                    indexAnalyzers
                );
            }
            return new DerivedFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), this, indexAnalyzers);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.getIndexAnalyzers()));
    private final String type;
    private final Script script;
    private final String sourceIndexedField;

    protected DerivedFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder,
        IndexAnalyzers indexAnalyzers
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.type = builder.type.getValue();
        this.script = builder.script.getValue();
        this.sourceIndexedField = builder.sourceIndexedField.getValue();
        this.indexAnalyzers = indexAnalyzers;
    }

    @Override
    public DerivedFieldType fieldType() {
        return (DerivedFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // Leaving this empty as the parsing should be handled via the Builder when root object is parsed.
        // The context would not contain anything in this case since the DerivedFieldMapper is not indexed or stored.
        throw new UnsupportedOperationException("should not be invoked");
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), this.indexAnalyzers).init(this);
    }

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

    public String getType() {
        return type;
    }

    public Script getScript() {
        return script;
    }

}
