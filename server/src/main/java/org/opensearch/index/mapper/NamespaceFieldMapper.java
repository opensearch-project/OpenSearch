package org.opensearch.index.mapper;


import org.opensearch.script.NamespaceScript;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.script.Script.DEFAULT_SCRIPT_LANG;

public class NamespaceFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "namespace";

    public static final Mapper.TypeParser PARSER = new TypeParser();

    private static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context) throws MapperParsingException {
            throw new IllegalStateException("NamespaceFieldMapper needs objbuilder to validate node");
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context, ObjectMapper.Builder objBuilder)
            throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, context, node);

            if (objBuilder != null) {
                Set<String> propertyFieldNames =
                    (Set<String>) objBuilder.mappersBuilders.stream()
                        .map(b -> ((Mapper.Builder) b).name())
                        .collect(Collectors.toSet());

                if (propertyFieldNames.containsAll(builder.fields.getValue()) == false) {
                    throw new MapperParsingException("[fields] should be from properties: [" + propertyFieldNames + "] but found [" + builder.fields.getValue() + "]");
                }
            }

            final Script s = builder.script.getValue();
            if (s != null) {
                NamespaceScript.Factory factory = context.scriptService().compile(builder.script.get(), NamespaceScript.CONTEXT);
                builder.compiledScript = factory.newInstance();
            }
            return builder;
        }
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private static NamespaceFieldMapper toType(FieldMapper in) {
            return (NamespaceFieldMapper) in;
        }

        private final Parameter<List<String>> fields = new Parameter<>(
            "fields",
            true,
            Collections::emptyList,
            (n, c, o) -> {
                if (!(o instanceof List)) {
                    throw new MapperParsingException( "Expected [fields] to be a list of strings but got [" + o + "]" );
                }

                List<String> fields = (List<String>) o;
                if (fields.isEmpty()) {
                    throw new MapperParsingException( "Expected [fields] to have one value" );
                }

                if (fields.size() > 1) {
                    throw new MapperParsingException( "Currently [fields] in namespace mapper does not support multiple values" );
                }

                return fields;
            },
            m -> toType(m).fields);

        private final Parameter<Script> script = new Parameter<>(
            "script",
            true,
            () -> null,
            (n, c, o) -> {
                if (o == null) {
                    return null;
                }

                Script s = Script.parse(o);
                if (!s.getLang().equals(DEFAULT_SCRIPT_LANG)) {
                    throw new MapperParsingException("Namespace only supports painless script" );
                }
                return s;
            },
            m -> toType(m).script
        ).acceptsNull();

        private NamespaceScript compiledScript;
        /**
         * Creates a new Builder with a field name
         *
         * @param name
         */
        protected Builder(String name) {
            super(name);
        }

        protected Builder(String name, List<String> fields, Script script, NamespaceScript namespaceScript) {
            super(name);
            this.fields.setValue(fields);
            this.script.setValue(script);
            this.compiledScript = namespaceScript;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(fields, script);
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            final NamespaceFieldType namespaceFieldType = new NamespaceFieldType(this.fields.getValue(), this.compiledScript);
            return new NamespaceFieldMapper(name, namespaceFieldType, this);
        }
    }


    private final List<String> fields;
    private final Script script;
    private final NamespaceScript compiledScript;
    /**
     * Creates a new ParametrizedFieldMapper
     *
     * @param simpleName
     * @param mappedFieldType
     * @param builder
     */
    protected NamespaceFieldMapper(String simpleName, NamespaceFieldType mappedFieldType, NamespaceFieldMapper.Builder builder) {
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
        throw new MapperParsingException("namespace field cannot be ingested in the document");
    }

    public NamespaceFieldType fieldType() {
        return (NamespaceFieldType) mappedFieldType;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
