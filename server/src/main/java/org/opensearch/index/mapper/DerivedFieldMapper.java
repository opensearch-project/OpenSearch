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
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A field mapper for derived fields
 *
 * @opensearch.internal
 */
public class DerivedFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "derived";

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
        private final Parameter<String> type = Parameter.stringParam("type", false, m -> toType(m).type, "");

        private final Parameter<Script> script = new Parameter<>(
            "script",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : Script.parse(o),
            m -> toType(m).script
        ).setSerializerCheck((id, ic, value) -> value != null);

        public Builder(String name) {
            super(name);
        }

        public Builder(DerivedField derivedField) {
            super(derivedField.getName());
            this.type.setValue(derivedField.getType());
            this.script.setValue(derivedField.getScript());
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(type, script);
        }

        @Override
        public DerivedFieldMapper build(BuilderContext context) {
            FieldMapper fieldMapper = DerivedFieldSupportedTypes.getFieldMapperFromType(type.getValue(), name, context);
            Function<Object, IndexableField> fieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
                type.getValue(),
                name
            );
            DerivedFieldType ft = new DerivedFieldType(
                new DerivedField(buildFullName(context), type.getValue(), script.getValue()),
                fieldMapper,
                fieldFunction
            );
            return new DerivedFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));
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
        return new Builder(simpleName()).init(this);
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

    public static Map<String, DerivedFieldType> getAllDerivedFieldTypeFromObject(
        Map<String, Object> derivedFieldObject,
        MapperService mapperService
    ) {
        Map<String, DerivedFieldType> derivedFieldTypes = new HashMap<>();
        DocumentMapper documentMapper = mapperService.documentMapperParser().parse(DerivedFieldMapper.CONTENT_TYPE, derivedFieldObject);
        if (documentMapper != null && documentMapper.mappers() != null) {
            for (Mapper mapper : documentMapper.mappers()) {
                if (mapper instanceof DerivedFieldMapper) {
                    DerivedFieldType derivedFieldType = ((DerivedFieldMapper) mapper).fieldType();
                    derivedFieldTypes.put(derivedFieldType.name(), derivedFieldType);
                }
            }
        }
        return derivedFieldTypes;
    }

    public static DerivedFieldType getDerivedFieldType(DerivedField derivedField, MapperService mapperService) {
        BuilderContext builderContext = new Mapper.BuilderContext(mapperService.getIndexSettings().getSettings(), new ContentPath(1));
        Builder builder = new Builder(derivedField);
        return builder.build(builderContext).fieldType();
    }
}
