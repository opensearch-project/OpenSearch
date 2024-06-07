/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.index.mapper.DateFieldMapper.getDefaultDateTimeFormatter;

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
        private final Parameter<String> type = Parameter.stringParam("type", true, m -> toType(m).type, "");
        private final IndexAnalyzers indexAnalyzers;
        private final boolean defaultIgnoreMalformed;
        private final DateFormatter defaultDateFormatter;

        private final Parameter<Script> script = new Parameter<>(
            "script",
            true,
            () -> null,
            (n, c, o) -> o == null ? null : Script.parse(o),
            m -> toType(m).script
        ).setSerializerCheck((id, ic, value) -> value != null);

        private final Parameter<Map<String, Object>> properties = new Parameter<>(
            "properties",
            true,
            Collections::emptyMap,
            (n, c, o) -> parseProperties(n, o),
            m -> toType(m).properties
        );
        private final Parameter<String> prefilterField = Parameter.stringParam("prefilter_field", true, m -> toType(m).prefilterField, "");
        private final Parameter<String> format = Parameter.stringParam(
            "format",
            true,
            m -> toType(m).format,
            getDefaultDateTimeFormatter().pattern()
        );
        private final Parameter<Boolean> ignoreMalformed;

        private static Map<String, Object> parseProperties(String name, Object propertiesObject) {
            if (propertiesObject instanceof Map == false) {
                throw new MapperParsingException(
                    "[properties] must be an object, got "
                        + propertiesObject.getClass().getSimpleName()
                        + "["
                        + propertiesObject
                        + "] for field ["
                        + name
                        + "]"
                );
            }
            @SuppressWarnings("unchecked")
            Map<String, ?> properties = (Map<String, ?>) propertiesObject;
            for (Object value : properties.values()) {
                if (value == null) {
                    throw new MapperParsingException("[properties] values can't be null (field [" + name + "])");
                } else if (!(value instanceof String)) {
                    // In the future, we can accept an Object too if needed
                    throw new MapperParsingException(
                        "[properties] values can only be strings, but got "
                            + value.getClass().getSimpleName()
                            + "["
                            + value
                            + "] for field ["
                            + name
                            + "]"
                    );
                }
            }
            return (Map<String, Object>) properties;
        }

        public Builder(String name, IndexAnalyzers indexAnalyzers, DateFormatter defaultDateFormatter, boolean defaultIgnoreMalformed) {
            super(name);
            this.indexAnalyzers = indexAnalyzers;
            this.defaultDateFormatter = defaultDateFormatter;
            this.defaultIgnoreMalformed = defaultIgnoreMalformed;
            if (defaultDateFormatter != null) {
                this.format.setValue(defaultDateFormatter.pattern());
            }
            this.ignoreMalformed = Parameter.boolParam("ignore_malformed", true, m -> toType(m).ignoreMalformed, defaultIgnoreMalformed);
        }

        public Builder(
            DerivedField derivedField,
            IndexAnalyzers indexAnalyzers,
            DateFormatter defaultDateFormatter,
            boolean defaultIgnoreMalformed
        ) {
            this(derivedField.getName(), indexAnalyzers, defaultDateFormatter, defaultIgnoreMalformed);
            this.type.setValue(derivedField.getType());
            this.script.setValue(derivedField.getScript());
            if (derivedField.getProperties() != null) {
                this.properties.setValue(derivedField.getProperties());
            }
            if (derivedField.getPrefilterField() != null) {
                this.prefilterField.setValue(derivedField.getPrefilterField());
            }
            if (derivedField.getFormat() != null) {
                this.format.setValue(derivedField.getFormat());
            }
            if (derivedField.getIgnoreMalformed()) {
                this.ignoreMalformed.setValue(derivedField.getIgnoreMalformed());
            }
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(type, script, properties, prefilterField, format, ignoreMalformed);
        }

        @Override
        public DerivedFieldMapper build(BuilderContext context) {
            DerivedField derivedField = new DerivedField(buildFullName(context), type.getValue(), script.getValue());
            if (properties.isConfigured()) {
                derivedField.setProperties(properties.getValue());
            }
            if (prefilterField.isConfigured()) {
                derivedField.setPrefilterField(prefilterField.getValue());
            }
            if (format.isConfigured()) {
                derivedField.setFormat(format.getValue());
            }
            if (ignoreMalformed.isConfigured()) {
                derivedField.setIgnoreMalformed(ignoreMalformed.getValue());
            }
            FieldMapper fieldMapper = DerivedFieldSupportedTypes.getFieldMapperFromType(type.getValue(), name, context, indexAnalyzers);
            Function<Object, IndexableField> fieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
                type.getValue(),
                name
            );
            DerivedFieldType ft;
            if (name.contains(".")) {
                ft = new ObjectDerivedFieldType(derivedField, fieldMapper, fieldFunction, indexAnalyzers);
            } else {
                ft = new DerivedFieldType(derivedField, fieldMapper, fieldFunction, indexAnalyzers);
            }
            return new DerivedFieldMapper(
                name,
                ft,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this,
                indexAnalyzers,
                defaultDateFormatter,
                defaultIgnoreMalformed
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(c.getSettings());
        return new Builder(n, c.getIndexAnalyzers(), c.getDateFormatter(), ignoreMalformedByDefault);
    });

    private final String type;
    private final Script script;
    private final String prefilterField;
    private final Map<String, Object> properties;
    private final boolean ignoreMalformed;
    private final boolean defaultIgnoreMalformed;
    private final DateFormatter defaultDateFormatter;
    private final String format;

    protected DerivedFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder,
        IndexAnalyzers indexAnalyzers,
        DateFormatter defaultDateFormatter,
        boolean ignoreMalformed
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.type = builder.type.getValue();
        this.script = builder.script.getValue();
        this.prefilterField = builder.prefilterField.getValue();
        this.properties = builder.properties.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.format = builder.format.getValue();
        this.indexAnalyzers = indexAnalyzers;
        this.defaultDateFormatter = defaultDateFormatter;
        this.defaultIgnoreMalformed = ignoreMalformed;
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
        return new Builder(simpleName(), this.indexAnalyzers, defaultDateFormatter, defaultIgnoreMalformed).init(this);
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
