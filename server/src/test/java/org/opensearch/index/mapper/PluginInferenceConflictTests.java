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
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests that core fails loudly when more than one plugin mechanism claims the same unmapped field —
 * two inferencers both claiming, or two plugin template types both matching. Per OpenSearch triage
 * (Froh): loop through all claimants and throw on ambiguity rather than letting registration/load
 * order silently pick a winner.
 */
public class PluginInferenceConflictTests extends MapperServiceTestCase {

    private static final String TYPE_A = "type_a";
    private static final String TYPE_B = "type_b";

    /** An inferencer that claims every numeric scalar as {@code claimedType}. */
    static class AlwaysClaimsNumberInferencer implements DynamicFieldTypeInferencer {
        private final String claimedType;

        AlwaysClaimsNumberInferencer(String claimedType) {
            this.claimedType = claimedType;
        }

        @Override
        public Set<String> supportedTypes() {
            return Collections.singleton(claimedType);
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

    /** A no-op template handler whose config is always complete (validated eagerly, no field read). */
    static class NoopHandler implements DynamicTemplateTypeHandler {
        @Override
        public void adjustMappingConfig(Map<String, Object> mappingConfig, FieldValueParserSupplier fieldValueParser) {}

        @Override
        public boolean isConfigComplete(Map<String, Object> mappingConfig) {
            return true;
        }
    }

    /** Builds a MockFieldMapper for a mock plugin type (no required params). */
    static class MockTypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) {
            node.remove("type");
            return new MockFieldMapper.Builder(name);
        }
    }

    /**
     * Registers two inferencers that both claim numeric scalars, and two plugin template types that
     * both match any field — so a single unmapped field is claimed by two mechanisms of each kind.
     */
    static class ConflictingPlugin extends Plugin implements MapperPlugin {
        @Override
        public List<DynamicFieldTypeInferencer> getDynamicFieldTypeInferencers() {
            return Arrays.asList(new AlwaysClaimsNumberInferencer(TYPE_A), new AlwaysClaimsNumberInferencer(TYPE_B));
        }

        @Override
        public Map<String, DynamicTemplateTypeHandler> getDynamicTemplateTypes() {
            Map<String, DynamicTemplateTypeHandler> handlers = new HashMap<>();
            handlers.put(TYPE_A, new NoopHandler());
            handlers.put(TYPE_B, new NoopHandler());
            return handlers;
        }

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            Map<String, Mapper.TypeParser> mappers = new HashMap<>();
            mappers.put(TYPE_A, new MockTypeParser());
            mappers.put(TYPE_B, new MockTypeParser());
            return mappers;
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new ConflictingPlugin());
    }

    public void testTwoInferencersClaimSameFieldThrows() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("n", 5))));
        assertThat(e.getMessage(), containsString("claimed by more than one dynamic field type inferencer"));
        assertThat(e.getMessage(), containsString("n"));
    }

    public void testTwoPluginTemplatesMatchSameFieldThrows() throws IOException {
        // Two templates, each targeting a different plugin type but the same field name pattern.
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("t_a");
            b.field("match_mapping_type", TYPE_A);
            b.field("match", "v_*");
            b.startObject("mapping").field("type", TYPE_A).endObject();
            b.endObject();
            b.endObject();
            b.startObject();
            b.startObject("t_b");
            b.field("match_mapping_type", TYPE_B);
            b.field("match", "v_*");
            b.startObject("mapping").field("type", TYPE_B).endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("v_x", 5))));
        assertThat(e.getMessage(), containsString("matched more than one dynamic template plugin type"));
        assertThat(e.getMessage(), containsString("v_x"));
    }

    public void testSingleTemplateMatchDoesNotThrow() throws IOException {
        // Sanity: with only ONE plugin template matching (a name pattern no other template shares),
        // there is no ambiguity — the field is claimed and mapped without a conflict exception.
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("only_a");
            b.field("match_mapping_type", TYPE_A);
            b.field("match", "only_*");
            b.startObject("mapping").field("type", TYPE_A).endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
        // "only_field" is a string, so the numeric inferencers don't fire; only template t_a matches.
        ParsedDocument doc = mapper.parse(source(b -> b.field("only_field", "text-value")));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull("single-matching template must map the field, not throw", doc.dynamicMappingsUpdate().root().getMapper("only_field"));
    }
}
