/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.script.ContextAwareGroupingScript;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.script.Script.DEFAULT_SCRIPT_LANG;

/**
 * A field mapper to specify context aware grouping mapper creation
 *
 * @opensearch.internal
 */
public class ContextAwareGroupingFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "context_aware_grouping";

    public static final Mapper.TypeParser PARSER = new TypeParser();

    private static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context) throws MapperParsingException {
            throw new IllegalStateException("ContextAwareGroupingFieldMapper needs objbuilder to validate node");
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context, ObjectMapper.Builder objBuilder)
            throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, context, node);

            if (builder.fields.isConfigured() == false) {
                throw new MapperParsingException("[fields] in context_aware_grouping is required");
            }

            Set<String> propertyFieldNames = (Set<String>) objBuilder.mappersBuilders.stream()
                .map(b -> ((Mapper.Builder) b).name())
                .collect(Collectors.toSet());

            if (propertyFieldNames.containsAll(builder.fields.getValue()) == false) {
                throw new MapperParsingException(
                    "[fields] should be from properties: [" + propertyFieldNames + "] but found [" + builder.fields.getValue() + "]"
                );
            }

            final Script s = builder.script.getValue();
            if (s != null) {
                ContextAwareGroupingScript.Factory factory = context.scriptService()
                    .compile(builder.script.get(), ContextAwareGroupingScript.CONTEXT);
                builder.compiledScript = factory.newInstance();
            }
            return builder;
        }
    }

    /**
     * Builder for this field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private static ContextAwareGroupingFieldMapper toType(FieldMapper in) {
            return (ContextAwareGroupingFieldMapper) in;
        }

        private final Parameter<List<String>> fields = new Parameter<>("fields", true, Collections::emptyList, (n, c, o) -> {
            if (!(o instanceof List)) {
                throw new MapperParsingException("Expected [fields] to be a list of strings but got [" + o + "]");
            }

            List<String> fields = (List<String>) o;
            if (fields.isEmpty()) {
                throw new MapperParsingException("Expected [fields] in context_aware_grouping to have one value");
            }

            if (fields.size() > 1) {
                throw new MapperParsingException("Currently [fields] in context_aware_grouping does not support multiple values");
            }

            return fields;
        }, m -> toType(m).fields);

        private final Parameter<Script> script = new Parameter<>("script", true, () -> null, (n, c, o) -> {
            if (o == null) {
                return null;
            }

            Script s = Script.parse(o);
            if (!s.getLang().equals(DEFAULT_SCRIPT_LANG)) {
                throw new MapperParsingException("context_aware_grouping only supports painless script");
            }
            return s;
        }, m -> toType(m).script).acceptsNull();

        private ContextAwareGroupingScript compiledScript;

        /**
         * Creates a new Builder with a field name
         *
         * @param name
         */
        protected Builder(String name) {
            super(name);
        }

        protected Builder(String name, List<String> fields, Script script, ContextAwareGroupingScript contextAwareGroupingScript) {
            super(name);
            this.fields.setValue(fields);
            this.script.setValue(script);
            this.compiledScript = contextAwareGroupingScript;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(fields, script);
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            final ContextAwareGroupingFieldType contextAwareGroupingFieldType = new ContextAwareGroupingFieldType(
                this.fields.getValue(),
                this.compiledScript
            );
            return new ContextAwareGroupingFieldMapper(name, contextAwareGroupingFieldType, this);
        }
    }

    private final List<String> fields;
    private final Script script;
    private final ContextAwareGroupingScript compiledScript;

    /**
     * Creates a new ParametrizedFieldMapper
     *
     * @param simpleName
     * @param mappedFieldType
     * @param builder
     */
    protected ContextAwareGroupingFieldMapper(
        String simpleName,
        ContextAwareGroupingFieldType mappedFieldType,
        ContextAwareGroupingFieldMapper.Builder builder
    ) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        this.fields = builder.fields.getValue();
        this.script = builder.script.getValue();
        this.compiledScript = builder.compiledScript;
    }

    @Override
    public Builder getMergeBuilder() {
        return new Builder(CONTENT_TYPE, this.fields, this.script, this.compiledScript);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new MapperParsingException("context_aware_grouping cannot be ingested in the document");
    }

    public ContextAwareGroupingFieldType fieldType() {
        return (ContextAwareGroupingFieldType) mappedFieldType;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Context Aware Segment field is not a part of an ingested document, so omitting it from Context Aware Segment
     * validation.
     */
    @Override
    public void canDeriveSource() {}

    /**
     * Context Aware Segment field is not a part of an ingested document, so omitting it from Context Aware Segment
     * generation.
     */
    @Override
    public void deriveSource(XContentBuilder builder, LeafReader leafReader, int docId) throws IOException {}
}
