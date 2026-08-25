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
    // what type plugin A's inferencer declares (supportedTypes) and what type it actually claims.
    private String pluginASupportedType;
    private String pluginAClaimedType;

    /**
     * Declares {@code supportedType} and claims numeric scalars as {@code claimedType}. The two are the
     * same in normal use; a test can set them differently to exercise the runtime check that rejects a
     * claim outside the declared supportedTypes().
     */
    static class ClaimsTypeInferencer implements DynamicFieldTypeInferencer {
        private final String supportedType;
        private final String claimedType;

        ClaimsTypeInferencer(String supportedType, String claimedType) {
            this.supportedType = supportedType;
            this.claimedType = claimedType;
        }

        @Override
        public Set<String> supportedTypes() {
            return Collections.singleton(supportedType);
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

    /**
     * Plugin A: registers and owns TYPE_A as a mapper type ONLY (no dynamic-template type). Its
     * inferencer declares {@link #pluginASupportedType} and claims {@link #pluginAClaimedType}.
     * Registering the type solely via {@link MapperPlugin#getMappers()} also proves ownership is bound
     * to the mapper registry, not to {@link MapperPlugin#getDynamicTemplateTypes()} — a pure
     * auto-inference plugin must still work.
     */
    class PluginA extends Plugin implements MapperPlugin {
        @Override
        public List<DynamicFieldTypeInferencer> getDynamicFieldTypeInferencers() {
            return Collections.singletonList(new ClaimsTypeInferencer(pluginASupportedType, pluginAClaimedType));
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
     * Plugin A declares supportedTypes() = TYPE_B, a type registered by plugin B, not A. Startup
     * validation rejects this: an inferencer may only declare types its own plugin registers via
     * getMappers(). This catches a cross-plugin type grab at node startup rather than at index time.
     */
    public void testInferencerDeclaringAnotherPluginsTypeFailsAtStartup() {
        pluginASupportedType = TYPE_B;
        pluginAClaimedType = TYPE_B;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(topMapping(b -> {})));
        assertThat(e.getMessage(), containsString("declares supported type [" + TYPE_B + "]"));
        assertThat(e.getMessage(), containsString("does not register via getMappers()"));
    }

    /**
     * Plugin A validly declares supportedTypes() = TYPE_A but its inferencer claims TYPE_B at runtime
     * (a type outside its declared set). The claim is rejected and the field falls through to normal
     * dynamic mapping (a numeric scalar maps as long). This is the runtime half of the contract.
     */
    public void testInferencerClaimingTypeOutsideSupportedTypesIsRejected() throws IOException {
        pluginASupportedType = TYPE_A;
        pluginAClaimedType = TYPE_B;
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("n", 5)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("n");
        assertNotNull(fieldMapper);
        assertThat("claim outside supportedTypes() must be rejected; field falls through to long", fieldMapper.typeName(), equalTo("long"));
    }

    /**
     * An inferencer producing a type it declared and its OWN plugin registered (as a mapper type only,
     * with NO dynamic-template type) is accepted. Guards against the binding being too strict and
     * covers a pure auto-inference plugin: if ownership were bound to {@code getDynamicTemplateTypes()}
     * instead of {@code getMappers()}, this valid claim would be silently rejected.
     */
    public void testInferencerCanProduceOwnPluginType() throws IOException {
        pluginASupportedType = TYPE_A;
        pluginAClaimedType = TYPE_A;
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("n", 5)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("n");
        assertNotNull(fieldMapper);
        assertThat("own declared+registered type must be accepted", fieldMapper.typeName(), equalTo("faketype"));
    }
}
