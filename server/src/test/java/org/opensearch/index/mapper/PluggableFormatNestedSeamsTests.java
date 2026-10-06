/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.FieldScope;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for the server-side pluggable data format seams around nested fields, exercised
 * through a fake {@link org.opensearch.index.engine.dataformat.DataFormatPlugin} and a
 * boundary-capturing {@link org.opensearch.index.engine.dataformat.DocumentInput} — no real
 * format plugin involved:
 *
 * <ul>
 *   <li>nested element boundary emission ({@code startNestedElement}/{@code endNestedElement})
 *       during document parsing, including the symmetric close when parsing an element fails,</li>
 *   <li>{@link FieldScope} propagation through the mapper tree during capability assignment.</li>
 * </ul>
 */
public class PluggableFormatNestedSeamsTests extends MapperServiceTestCase {

    private static final String FORMAT_NAME = "mock-format";

    /** Records the FieldScope each field type was assigned with, keyed by field name. */
    private final Map<String, FieldScope> recordedScopes = new LinkedHashMap<>();

    /** A DataFormatPlugin whose capability assignment only records the scope it was handed. */
    private class ScopeRecordingDataFormatPlugin extends MockDataFormatPlugin {
        ScopeRecordingDataFormatPlugin() {
            super(new MockDataFormat(FORMAT_NAME, 100L, Set.of()));
        }

        @Override
        public void assignCapabilities(
            MappedFieldType fieldType,
            IndexSettings indexSettings,
            DataFormatRegistry dataFormatRegistry,
            FieldScope fieldScope
        ) {
            recordedScopes.put(fieldType.name(), fieldScope);
            fieldType.setCapabilityMap(Map.of());
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new ScopeRecordingDataFormatPlugin());
    }

    /**
     * A capturing input that additionally records nested boundary signals, preserving the
     * relative order of field additions and boundary events.
     */
    static class BoundaryCapturingDocumentInput extends CapturingDocumentInput {
        record Event(String kind, String detail) {
        }

        private final List<Event> events = new ArrayList<>();

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            super.addField(fieldType, value);
            events.add(new Event("field", fieldType.name()));
        }

        @Override
        public void startNestedElement(String path) {
            events.add(new Event("start", path));
        }

        @Override
        public void endNestedElement() {
            events.add(new Event("end", null));
        }

        List<Event> boundaryEvents() {
            return events.stream().filter(e -> e.kind().equals("start") || e.kind().equals("end")).toList();
        }

        long count(String kind) {
            return events.stream().filter(e -> e.kind().equals(kind)).count();
        }

        int indexOf(String kind, String detail) {
            for (int i = 0; i < events.size(); i++) {
                if (events.get(i).kind().equals(kind) && java.util.Objects.equals(events.get(i).detail(), detail)) {
                    return i;
                }
            }
            return -1;
        }

        List<Event> events() {
            return events;
        }
    }

    private static Settings pluggableSettings() {
        return Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", FORMAT_NAME)
            .build();
    }

    // ------------------------------------------------------------------
    // Boundary emission
    // ------------------------------------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSingleNestedElementEmitsOneBoundaryPair() throws Exception {
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").field("index", false).endObject();
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject().field("name", "alice").endObject();
            b.endArray();
        }), input);

        assertEquals(1, input.count("start"));
        assertEquals(1, input.count("end"));
        assertEquals("user", input.boundaryEvents().get(0).detail());
        // The element's leaf must be emitted between the start and the end.
        int start = input.indexOf("start", "user");
        int field = input.indexOf("field", "user.name");
        int end = start + 1 + input.events()
            .subList(start + 1, input.events().size())
            .indexOf(new BoundaryCapturingDocumentInput.Event("end", null));
        assertTrue("leaf must be inside the element scope", start < field && field < end);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNestedArrayEmitsOnePairPerElementWithoutOverlap() throws Exception {
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").field("index", false).endObject();
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject().field("name", "alice").endObject();
            b.startObject().field("name", "bob").endObject();
            b.endArray();
        }), input);

        // Two elements: start,end,start,end — sibling scopes never overlap.
        List<BoundaryCapturingDocumentInput.Event> boundary = input.boundaryEvents();
        assertEquals(4, boundary.size());
        assertEquals("start", boundary.get(0).kind());
        assertEquals("end", boundary.get(1).kind());
        assertEquals("start", boundary.get(2).kind());
        assertEquals("end", boundary.get(3).kind());
        assertEquals("user", boundary.get(0).detail());
        assertEquals("user", boundary.get(2).detail());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPlainObjectEmitsNoBoundarySignals() throws Exception {
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("obj");
            {
                b.startObject("properties");
                b.startObject("inner").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        mapper.parse(source(b -> {
            b.startObject("obj");
            b.field("inner", "test");
            b.endObject();
        }), input);

        assertEquals(0, input.count("start"));
        assertEquals(0, input.count("end"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPlainObjectInsideNestedDoesNotOpenItsOwnScope() throws Exception {
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("address");
                    {
                        b.startObject("properties");
                        b.startObject("city").field("type", "keyword").field("index", false).endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject();
            {
                b.startObject("address").field("city", "seattle").endObject();
            }
            b.endObject();
            b.endArray();
        }), input);

        // Only the nested element brackets; the plain object contributes no signals, and its
        // leaf flows into the enclosing element's open scope.
        assertEquals(1, input.count("start"));
        assertEquals(1, input.count("end"));
        int start = input.indexOf("start", "user");
        int field = input.indexOf("field", "user.address.city");
        assertTrue("object child leaf must land inside the nested element scope", start >= 0 && start < field);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNestedInNestedEmitsProperlyBracketedSignals() throws Exception {
        // The server accepts nested-in-nested mappings (rejection, where a format cannot
        // support it, is plugin policy); the boundary stream must bracket correctly.
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("outer");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("inner");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        b.startObject("v").field("type", "keyword").field("index", false).endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        mapper.parse(source(b -> {
            b.startArray("outer");
            b.startObject();
            {
                b.startArray("inner");
                b.startObject().field("v", "x").endObject();
                b.endArray();
            }
            b.endObject();
            b.endArray();
        }), input);

        List<BoundaryCapturingDocumentInput.Event> boundary = input.boundaryEvents();
        assertEquals(4, boundary.size());
        assertEquals(new BoundaryCapturingDocumentInput.Event("start", "outer"), boundary.get(0));
        assertEquals(new BoundaryCapturingDocumentInput.Event("start", "outer.inner"), boundary.get(1));
        assertEquals("end", boundary.get(2).kind());
        assertEquals("end", boundary.get(3).kind());
    }

    public void testNoBoundarySignalsWhenPluggableFormatDisabled() throws Exception {
        // Same nested mapping and document, pluggable flag off: an input may still be passed
        // (it is simply propagated), but no boundary signals may be emitted.
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject().field("name", "alice").endObject();
            b.endArray();
        }), input);

        assertEquals(0, input.count("start"));
        assertEquals(0, input.count("end"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testEndSignalEmittedWhenElementParsingFails() throws Exception {
        // The end signal is emitted from a finally block, so the scope closes even when a leaf
        // inside the element fails to parse — consumers relying on strict start/end pairing
        // must not be left with a dangling open scope.
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("age").field("type", "integer").field("index", false).endObject();
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject().field("age", "not-a-number").endObject();
            b.endArray();
        }), input));

        assertEquals(1, input.count("start"));
        assertEquals("every start must have a matching end even on parse failure", input.count("start"), input.count("end"));
    }

    // ------------------------------------------------------------------
    // FieldScope propagation during capability assignment
    // ------------------------------------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testScopePropagationAcrossMapperTree() throws Exception {
        recordedScopes.clear();
        createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("root_leaf").field("type", "keyword").endObject();
            b.startObject("obj");
            {
                b.startObject("properties");
                b.startObject("obj_leaf").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("user");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").field("index", false).endObject();
                    b.startObject("address");
                    {
                        b.startObject("properties");
                        b.startObject("city").field("type", "keyword").field("index", false).endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Root-scope fields.
        assertEquals(FieldScope.ROOT, recordedScopes.get("root_leaf"));
        assertEquals("a plain object does not change scope", FieldScope.ROOT, recordedScopes.get("obj.obj_leaf"));
        // Nested-scope fields.
        assertEquals(FieldScope.NESTED, recordedScopes.get("user.name"));
        assertEquals(
            "scope must stay NESTED through a plain object inside nested",
            FieldScope.NESTED,
            recordedScopes.get("user.address.city")
        );
        // Metadata mappers are assigned at root scope.
        assertEquals(FieldScope.ROOT, recordedScopes.get("_id"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testScopePropagationOnMappingUpdate() throws Exception {
        // Capability assignment must re-run with correct scopes when a nested field arrives via
        // a mapping update (the merge path), not only at index creation.
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("root_leaf").field("type", "keyword").endObject();
        }));
        recordedScopes.clear();

        merge(mapperService, mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("text").field("type", "keyword").field("index", false).endObject();
                b.endObject();
            }
            b.endObject();
        }));

        assertEquals(FieldScope.NESTED, recordedScopes.get("comments.text"));
        assertEquals("pre-existing root fields keep ROOT scope on re-assignment", FieldScope.ROOT, recordedScopes.get("root_leaf"));
    }

    // ------------------------------------------------------------------
    // Vanilla behavior around the seams
    // ------------------------------------------------------------------

    public void testParseFailureInsideNestedElementSurfacesOriginalException() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.field("include_in_parent", true);
                b.startObject("properties");
                b.startObject("age").field("type", "integer").endObject();
                b.endObject();
            }
            b.endObject();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject().field("age", "not-a-number").endObject();
            b.endArray();
        })));
        assertTrue(
            "the original parse exception must surface, got: " + e.getMessage(),
            e.getMessage().contains("user.age") || e.getCause() != null && e.getCause().getMessage().contains("user.age")
        );
    }

    public void testVanillaNestedWithDisableObjectsKeepsFlattenedParsing() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.field("disable_objects", true);
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject().field("name", "alice").endObject();
            b.startObject().field("name", "bob").endObject();
            b.endArray();
        }));

        assertEquals("vanilla path must not create nested child documents", 1, doc.docs().size());
        assertTrue("values must flatten onto the root document", doc.rootDoc().getFields("user.name").length > 0);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableNestedWithDisableObjectsOpensElementScopes() throws Exception {
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("user");
            {
                b.field("type", "nested");
                b.field("disable_objects", true);
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").field("index", false).endObject();
                b.endObject();
            }
            b.endObject();
        }));
        BoundaryCapturingDocumentInput input = new BoundaryCapturingDocumentInput();

        mapper.parse(source(b -> {
            b.startArray("user");
            b.startObject().field("name", "alice").endObject();
            b.startObject().field("name", "bob").endObject();
            b.endArray();
        }), input);

        assertEquals("one element scope per array entry", 2, input.count("start"));
        assertEquals(2, input.count("end"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testFlatObjectAcceptedUnderPluggableDerivedSource() throws Exception {
        // Vanilla-side rejection: FlatObjectFieldMapperTests#testDerivedSourceRejectedOnVanillaIndex.
        createMapperService(pluggableSettings(), mapping(b -> { b.startObject("attrs").field("type", "flat_object").endObject(); }));
    }
}
