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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises the error/edge branches of the inferencer path in {@code DocumentParser.tryPluginInference}:
 * a buggy inferencer that throws is caught and skipped; an inferencer that returns a config with no
 * {@code type}, or an unknown {@code type}, falls through to the normal path instead of failing.
 *
 * <p>Each test installs a plugin with a single inferencer of a fixed behavior — no shared mutable
 * state — so the tests are independent and safe under any execution order.
 */
public class PluginInferencerEdgeCaseTests extends MapperServiceTestCase {

    enum Behavior {
        THROW,        // inferencer throws — must be caught, field falls through to normal path
        NO_TYPE,      // returns a config map without a "type" key — falls through
        UNKNOWN_TYPE  // returns a config with a type no TypeParser is registered for — falls through
    }

    private static final String DUMMY_TYPE = "dummy_type";

    /** Claims numeric scalars, then behaves per its fixed {@link Behavior}. Immutable, no shared state. */
    private static class ConfigurableInferencer implements DynamicFieldTypeInferencer {
        private final Behavior behavior;

        ConfigurableInferencer(Behavior behavior) {
            this.behavior = behavior;
        }

        @Override
        public Set<String> supportedTypes() {
            return Collections.singleton(DUMMY_TYPE);
        }

        @Override
        public Map<String, Object> inferFieldType(FieldValueParserSupplier fieldValueParser) throws IOException {
            try (XContentParser parser = fieldValueParser.get()) {
                if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) return null;
            }
            switch (behavior) {
                case THROW:
                    throw new RuntimeException("boom from inferencer");
                case NO_TYPE: {
                    Map<String, Object> config = new HashMap<>();
                    config.put("some_param", "x"); // deliberately no "type"
                    return config;
                }
                case UNKNOWN_TYPE:
                default: {
                    Map<String, Object> config = new HashMap<>();
                    config.put("type", "no_such_registered_type");
                    return config;
                }
            }
        }
    }

    private static class EdgeCasePlugin extends Plugin implements MapperPlugin {
        private final Behavior behavior;

        EdgeCasePlugin(Behavior behavior) {
            this.behavior = behavior;
        }

        @Override
        public List<DynamicFieldTypeInferencer> getDynamicFieldTypeInferencers() {
            return Collections.singletonList(new ConfigurableInferencer(behavior));
        }

        // Register DUMMY_TYPE so the inferencer's declared supportedTypes() passes startup validation.
        // The edge behaviors never successfully produce DUMMY_TYPE, so this parser is never exercised.
        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap(DUMMY_TYPE, (name, node, parserContext) -> {
                node.remove("type");
                return new MockFieldMapper.Builder(name);
            });
        }
    }

    // Set by each test before createDocumentMapper() is called; consumed by getPlugins().
    private Behavior behavior;

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new EdgeCasePlugin(behavior));
    }

    /** A throwing inferencer must not break parsing; the field falls through to normal inference (long). */
    public void testThrowingInferencerIsCaughtAndFieldFallsThrough() throws IOException {
        behavior = Behavior.THROW;
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("n", 5)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("n");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("long"));
    }

    /** A config with no "type" key is ignored; the field falls through to normal inference. */
    public void testInferredConfigWithoutTypeFallsThrough() throws IOException {
        behavior = Behavior.NO_TYPE;
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("n", 5)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("n");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("long"));
    }

    /** A config whose "type" has no registered TypeParser is ignored; the field falls through. */
    public void testInferredUnknownTypeFallsThrough() throws IOException {
        behavior = Behavior.UNKNOWN_TYPE;
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("n", 5)));
        assertNotNull(doc.dynamicMappingsUpdate());
        Mapper fieldMapper = doc.dynamicMappingsUpdate().root().getMapper("n");
        assertNotNull(fieldMapper);
        assertThat(fieldMapper.typeName(), equalTo("long"));
    }
}
