/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for plugin-extensible dynamic template types (match_mapping_type SPI)
 * and field type inferencers (DynamicFieldTypeInferencer SPI).
 *
 * Uses a minimal stub plugin:
 * - Registers "mock_type" as a plugin match_mapping_type
 * - Registers an inferencer that claims numeric scalars >= 100 as "long"
 *   (scalars are safe — Long mapper handles them without parsesArrayValue issues)
 */
public class PluginDynamicTemplateTests extends MapperServiceTestCase {

    private static final String MOCK_TYPE = "mock_type";
    // A plugin type whose handler always reads the field value, i.e. it depends on data-derived
    // parameters and therefore cannot be validated at index-creation time.
    private static final String MOCK_INFERRED_TYPE = "mock_inferred_type";
    // A plugin type whose handler reports its config complete but throws when normalizing it — used to
    // verify eager index-creation validation treats a handler contract violation as non-fatal (defer).
    private static final String MOCK_THROWING_TYPE = "mock_throwing_type";

    /** No-op handler — template config used as-is. Reports its config as always complete. */
    static class MockTemplateTypeHandler implements DynamicTemplateTypeHandler {
        @Override
        public void adjustMappingConfig(Map<String, Object> mappingConfig, FieldValueParserSupplier fieldValueParser) {}

        @Override
        public boolean isConfigComplete(Map<String, Object> mappingConfig) {
            return true;
        }
    }

    /** Handler for a data-derived type: config is never complete, so validation is deferred. */
    static class MockInferredTemplateTypeHandler implements DynamicTemplateTypeHandler {
        @Override
        public void adjustMappingConfig(Map<String, Object> mappingConfig, FieldValueParserSupplier fieldValueParser) throws IOException {
            try (XContentParser parser = fieldValueParser.get()) {
                parser.currentToken();
            }
        }

        @Override
        public boolean isConfigComplete(Map<String, Object> mappingConfig) {
            return false;
        }
    }

    /**
     * Reports its config as complete (so index-creation validation runs eagerly) but throws from
     * adjustMappingConfig — a handler-contract violation. Core must treat this as non-fatal and defer.
     */
    static class ThrowingTemplateTypeHandler implements DynamicTemplateTypeHandler {
        @Override
        public void adjustMappingConfig(Map<String, Object> mappingConfig, FieldValueParserSupplier fieldValueParser) {
            throw new IllegalStateException("handler contract violation");
        }

        @Override
        public boolean isConfigComplete(Map<String, Object> mappingConfig) {
            return true;
        }
    }

    // A numeric value the inferencer maps to the core type "long" (a type it does not register),
    // used to prove core rejects an inferred type no plugin owns.
    private static final int CORE_TYPE_SENTINEL = 999;

    /**
     * Claims numeric scalars >= 100 as the plugin-registered {@link #MOCK_TYPE}, except for the
     * {@link #CORE_TYPE_SENTINEL} value, which it claims as the core type "long" — a type it does
     * not register — so a test can assert core rejects an unregistered inferred type.
     * Using scalars (not arrays) avoids needing a parsesArrayValue mapper in core tests.
     */
    static class MockInferencer implements DynamicFieldTypeInferencer {
        @Override
        public Set<String> supportedTypes() {
            return Collections.singleton(MOCK_TYPE);
        }

        @Override
        public Map<String, Object> inferFieldType(FieldValueParserSupplier fieldValueParser) throws IOException {
            double value;
            try (XContentParser parser = fieldValueParser.get()) {
                if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) return null;
                value = parser.doubleValue();
                if (value < 100) return null;
            }
            Map<String, Object> config = new HashMap<>();
            if (value == CORE_TYPE_SENTINEL) {
                config.put("type", "long");
                return config;
            }
            config.put("type", MOCK_TYPE);
            config.put("required_param", "present");
            return config;
        }
    }

    /**
     * TypeParser for a plugin field type that requires a {@code required_param} and throws a
     * {@link MapperParsingException} when it is missing — mirroring how the knn_vector parser rejects
     * a mapping with no {@code dimension}. Used to prove eager index-creation-time validation throws.
     */
    static class RequiresParamTypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (node.containsKey("required_param") == false) {
                throw new MapperParsingException("required_param missing for field [" + name + "]");
            }
            node.remove("type");
            node.remove("required_param");
            return new MockFieldMapper.Builder(name);
        }
    }

    static class MockMapperPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, DynamicTemplateTypeHandler> getDynamicTemplateTypes() {
            Map<String, DynamicTemplateTypeHandler> handlers = new HashMap<>();
            handlers.put(MOCK_TYPE, new MockTemplateTypeHandler());
            handlers.put(MOCK_INFERRED_TYPE, new MockInferredTemplateTypeHandler());
            handlers.put(MOCK_THROWING_TYPE, new ThrowingTemplateTypeHandler());
            return handlers;
        }

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            Map<String, Mapper.TypeParser> mappers = new HashMap<>();
            mappers.put(MOCK_TYPE, new RequiresParamTypeParser());
            mappers.put(MOCK_INFERRED_TYPE, new RequiresParamTypeParser());
            mappers.put(MOCK_THROWING_TYPE, new RequiresParamTypeParser());
            return mappers;
        }

        @Override
        public List<DynamicFieldTypeInferencer> getDynamicFieldTypeInferencers() {
            return Collections.singletonList(new MockInferencer());
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new MockMapperPlugin());
    }

    // =====================================================================
    // DynamicTemplate unit tests
    // =====================================================================

    /** Parses a template with the mock plugin registry, so a plugin match_mapping_type is accepted. */
    private static DynamicTemplate parseWithRegistry(String name, Map<String, Object> conf) {
        Map<String, DynamicTemplateTypeHandler> registry = new HashMap<>();
        registry.put(MOCK_TYPE, new MockTemplateTypeHandler());
        registry.put(MOCK_INFERRED_TYPE, new MockInferredTemplateTypeHandler());
        registry.put(MOCK_THROWING_TYPE, new ThrowingTemplateTypeHandler());
        return DynamicTemplate.parse(name, conf, registry);
    }

    public void testPluginMatchTypeStoredOnParse() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", MOCK_TYPE);
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = parseWithRegistry("t", conf);
        assertEquals(MOCK_TYPE, template.getPluginMatchType());
        assertNull(template.getXContentFieldType());
    }

    public void testBuiltinMatchTypeNotStoredAsPlugin() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", "string");
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = DynamicTemplate.parse("t", conf);
        assertNull(template.getPluginMatchType());
        assertEquals(XContentFieldType.STRING, template.getXContentFieldType());
    }

    public void testAllBuiltinTypesNotStoredAsPlugin() {
        for (XContentFieldType t : XContentFieldType.values()) {
            Map<String, Object> conf = new HashMap<>();
            conf.put("match_mapping_type", t.toString());
            conf.put("mapping", Collections.singletonMap("type", "keyword"));
            DynamicTemplate template = DynamicTemplate.parse("t_" + t, conf);
            assertNull("builtin type " + t + " must not be stored as pluginMatchType", template.getPluginMatchType());
            assertEquals(t, template.getXContentFieldType());
        }
    }

    public void testMatchesPluginTypeTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", MOCK_TYPE);
        conf.put("match", "big_*");
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = parseWithRegistry("t", conf);
        assertTrue(template.matchesPluginType("big_field", "big_field", MOCK_TYPE));
    }

    public void testMatchesPluginTypeFalseWrongType() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", MOCK_TYPE);
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = parseWithRegistry("t", conf);
        assertFalse(template.matchesPluginType("field", "field", "other_type"));
    }

    public void testMatchesPluginTypeFalseNamePatternMismatch() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", MOCK_TYPE);
        conf.put("match", "big_*");
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = parseWithRegistry("t", conf);
        assertFalse(template.matchesPluginType("small_field", "small_field", MOCK_TYPE));
    }

    public void testMatchesPluginTypeWithPathMatch() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", MOCK_TYPE);
        conf.put("path_match", "obj.*");
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = parseWithRegistry("t", conf);
        assertTrue(template.matchesPluginType("obj.field", "field", MOCK_TYPE));
        assertFalse(template.matchesPluginType("other.field", "field", MOCK_TYPE));
    }

    public void testMatchesPluginTypeWithUnmatch() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", MOCK_TYPE);
        conf.put("unmatch", "excluded_*");
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = parseWithRegistry("t", conf);
        assertTrue(template.matchesPluginType("big_field", "big_field", MOCK_TYPE));
        assertFalse(template.matchesPluginType("excluded_field", "excluded_field", MOCK_TYPE));
    }

    public void testBuiltinMatchNeverReturnsPluginTemplate() throws IOException {
        // findTemplate() on RootObjectMapper must skip plugin templates
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("mock_template");
            b.field("match_mapping_type", MOCK_TYPE);
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
        RootObjectMapper root = mapperService.documentMapper().mapping().root();
        ContentPath path = new ContentPath();
        for (XContentFieldType t : XContentFieldType.values()) {
            assertNull("findTemplate must not return plugin template for builtin type " + t, root.findTemplate(path, "any_field", t));
        }
    }

    public void testPluginTemplateSerializesMatchMappingType() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", MOCK_TYPE);
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = parseWithRegistry("t", conf);
        XContentBuilder builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(builder.toString(), containsString("\"match_mapping_type\":\"" + MOCK_TYPE + "\""));
    }

    public void testWildcardNotStoredAsPlugin() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("match_mapping_type", "*");
        conf.put("mapping", Collections.singletonMap("type", "keyword"));
        DynamicTemplate template = DynamicTemplate.parse("t", conf);
        assertNull(template.getPluginMatchType());
        assertNull(template.getXContentFieldType());
    }

    // =====================================================================
    // Index creation validation tests
    // =====================================================================

    public void testUnregisteredPluginTypeThrowsAtIndexCreation() throws IOException {
        XContentBuilder mapping = topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("bad_template");
            b.field("match_mapping_type", "unregistered_type");
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        });
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getMessage(), containsString("No field type matched on [unregistered_type]"));
        assertThat(e.getMessage(), containsString(MOCK_TYPE));
        assertThat(e.getMessage(), containsString("string"));
        assertThat(e.getMessage(), containsString("long"));
    }

    public void testTypoInPluginTypeThrowsWithFullList() throws IOException {
        XContentBuilder mapping = topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("typo_template");
            b.field("match_mapping_type", "mock_typo");
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        });
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getMessage(), containsString("mock_typo"));
        assertThat(e.getMessage(), containsString(MOCK_TYPE));
    }

    public void testRegisteredPluginTypeAcceptedAtIndexCreation() throws IOException {
        createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("mock_template");
            b.field("match_mapping_type", MOCK_TYPE);
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
    }

    public void testBuiltinTypeStillWorksAlongsidePluginType() throws IOException {
        createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("string_template");
            b.field("match_mapping_type", "string");
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject();
            b.startObject("mock_template");
            b.field("match_mapping_type", MOCK_TYPE);
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
    }

    // =====================================================================
    // Eager plugin-template validation at index creation
    // =====================================================================

    public void testCompletePluginTemplateWithValidConfigAccepted() throws IOException {
        // Handler opens no parser (complete config) and the type parser is satisfied → index creation succeeds.
        createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("mock_template");
            b.field("match_mapping_type", MOCK_TYPE);
            b.startObject("mapping");
            b.field("type", MOCK_TYPE);
            b.field("required_param", "present");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
    }

    public void testCompletePluginTemplateWithInvalidConfigThrowsAtIndexCreation() throws IOException {
        // Complete config (handler opens no parser) but required_param is missing → the type parser
        // throws at index creation instead of silently accepting the broken template.
        XContentBuilder mapping = topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("mock_template");
            b.field("match_mapping_type", MOCK_TYPE);
            b.startObject("mapping");
            b.field("type", MOCK_TYPE);
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        });
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getMessage(), containsString("required_param missing"));
    }

    public void testInferredPluginTemplateWithIncompleteConfigNotValidatedAtIndexCreation() throws IOException {
        // The handler for this type always opens the parser (data-derived config), so even though
        // required_param is absent the template must NOT be validated/rejected at index creation.
        createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("inferred_template");
            b.field("match_mapping_type", MOCK_INFERRED_TYPE);
            b.startObject("mapping");
            b.field("type", MOCK_INFERRED_TYPE);
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
    }

    public void testHandlerThrowingDuringEagerValidationIsNonFatal() throws IOException {
        // The handler reports its config complete but throws from adjustMappingConfig. Core must treat
        // this contract violation as non-fatal (log + defer) rather than failing index creation.
        createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("throwing_template");
            b.field("match_mapping_type", MOCK_THROWING_TYPE);
            b.startObject("mapping");
            b.field("type", MOCK_THROWING_TYPE);
            b.field("required_param", "present");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
    }

    public void testPluginTemplateWithNamePlaceholderSkipsEagerValidation() throws IOException {
        // {name} can't be resolved up front, so validation is skipped even though required_param is missing.
        createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("named_template");
            b.field("match_mapping_type", MOCK_TYPE);
            b.startObject("mapping");
            b.field("type", MOCK_TYPE);
            b.field("field_name", "{name}");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
    }

    // =====================================================================
    // Document indexing — inferencer tests
    // =====================================================================

    public void testInferencerClaimsLargeNumericScalar() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        // 200 >= 100 threshold — MockInferencer claims it as the plugin-registered MOCK_TYPE
        ParsedDocument doc = mapper.parse(source(b -> b.field("big_number", 200)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("big_number");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("faketype"));
    }

    public void testInferencerReturningCoreTypeIsRejected() throws IOException {
        // The inferencer returns the core type "long" (which it does not register) for the sentinel
        // value. Core must ignore the claim and let the normal dynamic-mapping path map it as long.
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("big_number", CORE_TYPE_SENTINEL)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("big_number");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("long"));
    }

    public void testInferencerDoesNotClaimSmallNumericScalar() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        // 50 < 100 threshold — MockInferencer returns null, normal path maps as long
        ParsedDocument doc = mapper.parse(source(b -> b.field("small_number", 50)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("small_number");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("long"));
    }

    public void testInferencerDoesNotClaimString() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        // String — MockInferencer returns null (not a Number), normal path maps as text
        ParsedDocument doc = mapper.parse(source(b -> b.field("str_field", "hello")));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("str_field");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("text"));
    }

    public void testInferencerDoesNotClaimBoolean() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("bool_field", true)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("bool_field");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("boolean"));
    }

    public void testExplicitMappingBeatsInference() throws IOException {
        // Explicit keyword mapping — inference must not fire even for value >= 100
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startObject("properties");
            b.startObject("my_field");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("my_field", 200)));
        assertNull("explicit mapping must beat inferencer", doc.dynamicMappingsUpdate());
    }

    public void testAlreadyInferredFieldSkipsInferenceOnSecondDoc() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        // First doc triggers inference
        ParsedDocument first = mapper.parse(source(b -> b.field("big_number", 200)));
        assertNotNull(first.dynamicMappingsUpdate());

        // Simulate mapping update applied — second doc should not re-infer
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            b.startObject("big_number");
            b.field("type", "long");
            b.endObject();
            b.endObject();
        }));
        ParsedDocument second = mapperService.documentMapper().parse(source(b -> b.field("big_number", 300)));
        assertNull("already mapped field must not re-trigger inference", second.dynamicMappingsUpdate());
    }

    public void testNoPluginsNormalPathRuns() throws IOException {
        // Without plugins, tryPluginInference returns false immediately
        MapperService noPluginService = new MapperServiceTestCase() {
            @Override
            protected Collection<? extends Plugin> getPlugins() {
                return Collections.emptyList();
            }
        }.createMapperService(topMapping(b -> {}));

        ParsedDocument doc = noPluginService.documentMapper().parse(source(b -> b.field("my_field", "hello")));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("my_field");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("text"));
    }

    public void testBuiltinTemplateFallbackWhenInferencerDoesNotClaim() throws IOException {
        // Builtin string → keyword template still fires when inferencer doesn't claim
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("strings_as_keyword");
            b.field("match_mapping_type", "string");
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("my_field", "hello")));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("my_field");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("keyword"));
    }

    /** Plugin that registers only a template type handler — no inferencer */
    static class TemplateOnlyPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, DynamicTemplateTypeHandler> getDynamicTemplateTypes() {
            return Collections.singletonMap(MOCK_TYPE, new MockTemplateTypeHandler());
        }
    }

    public void testFastPathNotExitedWhenOnlyTemplateTypesRegistered() throws IOException {
        // Plugin registers template type but no inferencer.
        // tryPluginInference must NOT fast-exit — template types alone are enough to proceed.
        MapperService mapperService = new MapperServiceTestCase() {
            @Override
            protected Collection<? extends Plugin> getPlugins() {
                return Collections.singletonList(new TemplateOnlyPlugin());
            }
        }.createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("mock_template");
            b.field("match_mapping_type", MOCK_TYPE);
            b.startObject("mapping");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));
        assertNotNull(mapperService.documentMapper());
    }
}
