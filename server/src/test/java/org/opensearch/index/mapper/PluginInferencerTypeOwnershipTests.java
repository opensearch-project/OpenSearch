/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that a dynamic field type inferencer may only produce a type its OWN plugin registered.
 *
 * <p>Two plugins are installed: plugin B registers the type {@code type_b}, and plugin A ships an
 * inferencer whose claimed type is chosen per-test. When A claims {@code type_b} — a type it does not
 * own — core rejects the claim and falls through to normal dynamic mapping, even though {@code type_b}
 * is a validly-registered type globally. When A claims its own {@code type_a}, the claim is accepted.
 * This is the gap the coarse "is it any plugin-registered type" check did not cover.
 */
public class PluginInferencerTypeOwnershipTests extends MapperServiceTestCase {

    private static final String TYPE_A = "type_a";
    private static final String TYPE_B = "type_b";

    // Set by each test before createDocumentMapper() is called; consumed by getPlugins() to decide
    // which type plugin A's inferencer claims.
    private String pluginAClaimedType;

    /** Claims numeric scalars as {@code claimedType}. */
    static class ClaimsTypeInferencer implements DynamicFieldTypeInferencer {
        private final String claimedType;

        ClaimsTypeInferencer(String claimedType) {
            this.claimedType = claimedType;
        }

        @Override
        public Map<String, Object> inferFieldType(FieldValueParserSupplier fieldValueParser) throws IOException {
            try (XContentParser parser = fieldValueParser.get()) {
                if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) return null;
            }
            Map<String, Object> config = new HashMap<>();
            config.put("type", claimedType);
            return config;
        }
    }

    /** No-op handler; its config is always complete so no field read happens. */
    static class NoopHandler implements DynamicTemplateTypeHandler {
        @Override
        public void adjustMappingConfig(Map<String, Object> mappingConfig, FieldValueParserSupplier fieldValueParser) {}

        @Override
        public boolean isConfigComplete(Map<String, Object> mappingConfig) {
            return true;
        }
    }

    /** Builds a MockFieldMapper (no required params). */
    static class MockTypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) {
            node.remove("type");
            return new MockFieldMapper.Builder(name);
        }
    }

    /** Plugin A: registers and owns TYPE_A; its inferencer claims whatever type the test selected. */
    class PluginA extends Plugin implements MapperPlugin {
        @Override
        public List<DynamicFieldTypeInferencer> getDynamicFieldTypeInferencers() {
            return Collections.singletonList(new ClaimsTypeInferencer(pluginAClaimedType));
        }

        @Override
        public Map<String, DynamicTemplateTypeHandler> getDynamicTemplateTypes() {
            return Collections.singletonMap(TYPE_A, new NoopHandler());
        }

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap(TYPE_A, new MockTypeParser());
        }
    }

    /** Plugin B: registers and owns TYPE_B. Has no inferencer. */
    static class PluginB extends Plugin implements MapperPlugin {
        @Override
        public Map<String, DynamicTemplateTypeHandler> getDynamicTemplateTypes() {
            return Collections.singletonMap(TYPE_B, new NoopHandler());
        }

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap(TYPE_B, new MockTypeParser());
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Arrays.asList(new PluginA(), new PluginB());
    }

    /**
     * Plugin A's inferencer returns TYPE_B, which is registered by plugin B, not A. Even though
     * TYPE_B is a valid plugin type globally, A does not own it, so the claim is rejected and the
     * field falls through to normal dynamic mapping (a numeric scalar maps as long).
     */
    public void testInferencerCannotProduceAnotherPluginsType() throws IOException {
        pluginAClaimedType = TYPE_B;
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("n", 5)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("n");
        assertNotNull(fieldMapper);
        assertThat("borrowed type must be rejected; field falls through to long", fieldMapper.typeName(), equalTo("long"));
    }

    /**
     * An inferencer producing a type its OWN plugin registered is accepted even when a second plugin
     * is also installed. Guards against the ownership binding being too strict.
     */
    public void testInferencerCanProduceOwnPluginType() throws IOException {
        pluginAClaimedType = TYPE_A;
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("n", 5)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("n");
        assertNotNull(fieldMapper);
        assertThat("own registered type must be accepted", fieldMapper.typeName(), equalTo("faketype"));
    }
}
