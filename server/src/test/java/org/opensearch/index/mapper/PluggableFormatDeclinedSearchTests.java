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
import org.opensearch.common.Explicit;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.FieldScope;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The server records a data format's declined search capability in the stored mapping: {@code index: false}
 * on a nested object when all of its searchable leaves were declined (leaves then carry no {@code index}),
 * otherwise on the field itself (root {@code flat_object}, or a leaf in a mixed nested scope).
 *
 * <p>The fake plugin applies the composite plugin's policy: search declined inside nested and for a root
 * flat_object, everything else claimed in full. A switch grants numerics search under nested for the
 * mixed case.
 */
public class PluggableFormatDeclinedSearchTests extends MapperServiceTestCase {

    private static final String FORMAT_NAME = "mock-format";

    private static final Set<Capability> STORAGE_SHAPED = EnumSet.of(Capability.COLUMNAR_STORAGE, Capability.STORED_FIELDS);

    private boolean grantKeywordSearchUnderNested = false;
    private boolean declineRootKeywordSearch = false;

    private class DecliningDataFormatPlugin extends MockDataFormatPlugin {
        DecliningDataFormatPlugin() {
            super(new MockDataFormat(FORMAT_NAME, 100L, Set.of()));
        }

        @Override
        public void assignCapabilities(
            MappedFieldType fieldType,
            IndexSettings indexSettings,
            DataFormatRegistry dataFormatRegistry,
            FieldScope fieldScope
        ) {
            Set<Capability> requested = fieldType.requestedCapabilities();
            boolean keyword = fieldType instanceof KeywordFieldMapper.KeywordFieldType;
            boolean declineSearch = (fieldScope == FieldScope.NESTED && (keyword == false || grantKeywordSearchUnderNested == false))
                || FlatObjectFieldMapper.CONTENT_TYPE.equals(fieldType.typeName())
                || (declineRootKeywordSearch && fieldScope == FieldScope.ROOT && keyword);
            Set<Capability> granted = declineSearch
                ? requested.stream().filter(STORAGE_SHAPED::contains).collect(Collectors.toSet())
                : requested;
            fieldType.setCapabilityMap(granted.isEmpty() ? Map.of() : Map.of(getDataFormat(), Set.copyOf(granted)));
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new DecliningDataFormatPlugin());
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> storedMapping(MapperService mapperService) {
        return XContentHelper.convertToMap(
            mapperService.documentMapper().mappingSource().compressedReference(),
            true,
            MediaTypeRegistry.JSON
        ).v2();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> field(Map<String, Object> mapping, String propertiesPath) {
        Object node = XContentMapValues.extractValue("_doc.properties." + propertiesPath, mapping);
        assertNotNull("no mapping node at [" + propertiesPath + "] in " + mapping, node);
        return (Map<String, Object>) node;
    }

    private static void assertNestedNotIndexed(Map<String, Object> stored, String path) {
        Map<String, Object> node = field(stored, path);
        assertEquals("nested", node.get("type"));
        assertEquals("nested object [" + path + "] must carry index:false", false, node.get("index"));
    }

    /** Under a flagged nested object no leaf carries {@code index}: an explicit one is rejected, never rewritten. */
    private static void assertNoIndexKey(Map<String, Object> stored, String path) {
        assertFalse("[" + path + "] must carry no index key", field(stored, path).containsKey("index"));
    }

    private static void assertRejected(String expectedFragment, ThrowingRunnable runnable) {
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertTrue(e.getMessage(), e.getMessage().contains(expectedFragment));
    }

    /** Nested with two searchable leaves at their type defaults (keyword indexed; numeric not, on a pluggable index). */
    private XContentBuilder nestedWithSearchableLeaves() throws IOException {
        return mapping(b -> {
            b.startObject("service").field("type", "keyword").endObject();
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.startObject("code").field("type", "integer").endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    /** Same, but the numeric leaf spells out {@code index: true} so it is searchable and can be granted. */
    private XContentBuilder nestedWithExplicitNumericLeaf() throws IOException {
        return mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.startObject("code").field("type", "integer").field("index", true).endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    // ------------------------------------------------------------------
    // Nested: one flag on the object, no index key on any leaf
    // ------------------------------------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDeclinedNestedScopeIsRecordedOnTheNestedObject() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), nestedWithSearchableLeaves());
        Map<String, Object> stored = storedMapping(mapperService);

        assertNestedNotIndexed(stored, "events");
        assertNoIndexKey(stored, "events.properties.name");
        assertNoIndexKey(stored, "events.properties.code");
        assertNoIndexKey(stored, "service");

        assertFalse(mapperService.getObjectMapper("events").nested().isIndexed());
        assertTrue(mapperService.fieldType("events.name").isSearchable());
        assertFalse(mapperService.fieldType("events.code").isSearchable());
        assertEquals(
            Set.of(Capability.COLUMNAR_STORAGE),
            mapperService.fieldType("events.name").getCapabilityMap().values().iterator().next()
        );
        assertTrue(mapperService.fieldType("service").isSearchable());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitIndexTrueLeafUnderDeclinedNestedIsRejected() throws Exception {
        assertRejected(
            "field [events.code] must not set [index]: the data format cannot search under nested object [events]",
            () -> createMapperService(pluggableSettings(), nestedWithExplicitNumericLeaf())
        );
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitIndexFalseLeafUnderDeclinedNestedIsRejected() throws Exception {
        assertRejected("field [events.tag] must not set [index]", () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.startObject("tag").field("type", "keyword").field("index", false).field("doc_values", true).endObject();
                b.endObject();
            }
            b.endObject();
        })));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitIndexOnMultiFieldUnderDeclinedNestedIsRejected() throws Exception {
        assertRejected("field [events.name.raw] must not set [index]", () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name");
                {
                    b.field("type", "keyword");
                    b.startObject("fields");
                    b.startObject("raw").field("type", "keyword").field("index", true).endObject();
                    b.endObject();
                }
                b.endObject();
                b.endObject();
            }
            b.endObject();
        })));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMultiFieldsUnderNestedCarryNoIndexKey() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name");
                {
                    b.field("type", "keyword");
                    b.startObject("fields");
                    b.startObject("raw").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Map<String, Object> stored = storedMapping(mapperService);

        assertNestedNotIndexed(stored, "events");
        assertFalse(field(stored, "events.properties.name").containsKey("index"));
        assertFalse(field(stored, "events.properties.name.fields.raw").containsKey("index"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOnlyTheOutermostNestedObjectIsFlagged() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("outer");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("plain");
                    {
                        b.startObject("properties");
                        b.startObject("city").field("type", "keyword").endObject();
                        b.endObject();
                    }
                    b.endObject();
                    b.startObject("inner");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        b.startObject("v").field("type", "keyword").endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        Map<String, Object> stored = storedMapping(mapperService);

        assertNestedNotIndexed(stored, "outer");
        assertFalse("a plain object inside nested carries nothing", field(stored, "outer.properties.plain").containsKey("index"));
        assertFalse("an inner nested object inherits the outer flag", field(stored, "outer.properties.inner").containsKey("index"));
        assertFalse(field(stored, "outer.properties.plain.properties.city").containsKey("index"));
        assertFalse(field(stored, "outer.properties.inner.properties.v").containsKey("index"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNestedWithoutSearchableLeavesIsFlagged() throws Exception {
        // integer defaults to index:false on a pluggable index; the flag follows the format, not the leaves present.
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("code").field("type", "integer").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("empty").field("type", "nested").endObject();
        }));
        Map<String, Object> stored = storedMapping(mapperService);
        assertNestedNotIndexed(stored, "events");
        assertNestedNotIndexed(stored, "empty");
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testLeavesAddedLaterToAnEmptyNestedObjectAreAccepted() throws Exception {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("events").field("type", "nested").endObject())
        );
        merge(mapperService, mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.startObject("code").field("type", "integer").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Map<String, Object> stored = storedMapping(mapperService);
        assertNestedNotIndexed(stored, "events");
        assertNoIndexKey(stored, "events.properties.name");
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitIndexOnInnerNestedObjectIsRejected() throws Exception {
        assertRejected("nested object [outer.inner] must not set [index]", () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("outer");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("inner").field("type", "nested").field("index", false).endObject();
                b.endObject();
            }
            b.endObject();
        })));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMixedNestedScopeIsRejected() throws Exception {
        // The format searches keyword under nested but not text: one object flag cannot describe that.
        grantKeywordSearchUnderNested = true;
        assertRejected(
            "field [events.note]: the data format supports search for some fields under nested object [events] but not others",
            () -> createMapperService(pluggableSettings(), mapping(b -> {
                b.startObject("events");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    b.startObject("name").field("type", "keyword").endObject();
                    b.startObject("note").field("type", "text").endObject();
                    b.endObject();
                }
                b.endObject();
            }))
        );
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNestedScopeTheFormatCanSearchIsLeftAsWritten() throws Exception {
        grantKeywordSearchUnderNested = true;
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.startObject("tag").field("type", "keyword").field("index", false).endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Map<String, Object> stored = storedMapping(mapperService);
        assertFalse(field(stored, "events").containsKey("index"));
        assertEquals(false, field(stored, "events.properties.tag").get("index"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDeclinedSearchOutsideNestedAndFlatObjectIsRejected() throws Exception {
        declineRootKeywordSearch = true;
        assertRejected(
            "field [service] of type [keyword]: the data format cannot search it, which is only supported for nested objects and flat_object fields",
            () -> createMapperService(pluggableSettings(), mapping(b -> b.startObject("service").field("type", "keyword").endObject()))
        );
    }

    // ------------------------------------------------------------------
    // flat_object: the leaf itself
    // ------------------------------------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDeclinedSearchOnRootFlatObjectIsStoredAsIndexFalse() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("attrs").field("type", "flat_object").endObject();
        }));
        Map<String, Object> stored = storedMapping(mapperService);

        assertEquals("flat_object", field(stored, "attrs").get("type"));
        assertEquals(false, field(stored, "attrs").get("index"));
        assertFalse(mapperService.fieldType("attrs").isSearchable());
        FlatObjectFieldMapper.FlatObjectFieldType fft = (FlatObjectFieldMapper.FlatObjectFieldType) mapperService.fieldType("attrs");
        assertFalse(fft.getValueFieldType().isSearchable());
        assertFalse(fft.getValueAndPathFieldType().isSearchable());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitIndexTrueOnRootFlatObjectIsRejected() throws Exception {
        assertRejected("field [attrs] must not set [index: true]", () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("attrs").field("type", "flat_object").field("index", true).endObject();
        })));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitIndexFalseOnRootFlatObjectIsRejected() throws Exception {
        assertRejected("[index] on [attrs] is set by the server", () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("attrs").field("type", "flat_object").field("index", false).endObject();
        })));
    }

    // ------------------------------------------------------------------
    // The stored index:false markers
    // ------------------------------------------------------------------

    private XContentBuilder nestedAndFlatObject() throws IOException {
        return mapping(b -> {
            b.startObject("attrs").field("type", "flat_object").endObject();
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testStoredMarkersAreAcceptedOnRecovery() throws Exception {
        CompressedXContent stored = createMapperService(pluggableSettings(), nestedAndFlatObject()).documentMapper().mappingSource();
        MapperService recovered = createMapperService(pluggableSettings(), mapping(b -> {}));
        recovered.merge(MapperService.SINGLE_MAPPING_NAME, stored, MergeReason.MAPPING_RECOVERY);
        assertEquals(stored, recovered.documentMapper().mappingSource());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRePuttingTheStoredMappingOnTheSameIndexIsAccepted() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), nestedAndFlatObject());
        CompressedXContent stored = mapperService.documentMapper().mappingSource();
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, stored, MergeReason.MAPPING_UPDATE);
        assertEquals(stored, mapperService.documentMapper().mappingSource());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicUpdateCarryingTheParentFlagIsAccepted() throws Exception {
        // A dynamic update repeats the parent nested object, including its recorded index:false.
        MapperService mapperService = createMapperService(pluggableSettings(), nestedAndFlatObject());
        merge(mapperService, MergeReason.MAPPING_UPDATE_PREFLIGHT, dynamicUpdateUnderEvents());
        merge(mapperService, dynamicUpdateUnderEvents());
        assertNoIndexKey(storedMapping(mapperService), "events.properties.dyn");
    }

    private XContentBuilder dynamicUpdateUnderEvents() throws IOException {
        return mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested").field("index", false);
                b.startObject("properties");
                b.startObject("dyn").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testCopyingTheStoredMappingIntoANewIndexIsRejected() throws Exception {
        CompressedXContent stored = createMapperService(pluggableSettings(), nestedAndFlatObject()).documentMapper().mappingSource();
        MapperService fresh = createMapperService(pluggableSettings(), mapping(b -> {}));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> fresh.merge(MapperService.SINGLE_MAPPING_NAME, stored, MergeReason.INDEX_TEMPLATE)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("is set by the server and must not be specified in the mapping"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testTemplateLayerRepeatingAnEarlierLayersFlagIsRejected() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(b -> {}));
        merge(mapperService, MergeReason.INDEX_TEMPLATE, nestedAndFlatObject());
        assertRejected(
            "[index] on [events] is set by the server",
            () -> merge(mapperService, MergeReason.INDEX_TEMPLATE, dynamicUpdateUnderEvents())
        );
    }

    // ------------------------------------------------------------------
    // Stability of the rewritten mapping
    // ------------------------------------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRewrittenMappingIsAFixedPoint() throws Exception {
        // What a data node does on start; also what MapperService#assertSerialization checks under -ea.
        MapperService mapperService = createMapperService(pluggableSettings(), nestedWithSearchableLeaves());
        DocumentMapper first = mapperService.documentMapper();
        DocumentMapper reparsed = mapperService.parse(first.type(), first.mappingSource());
        assertEquals(first.mappingSource(), reparsed.mappingSource());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRePuttingTheOriginalMappingDoesNotConflict() throws Exception {
        // The incoming mapping is aligned the same way, so the merge sees index:false against index:false.
        MapperService mapperService = createMapperService(pluggableSettings(), nestedWithSearchableLeaves());
        DocumentMapper before = mapperService.documentMapper();

        merge(mapperService, nestedWithSearchableLeaves());

        assertEquals(before.mappingSource(), mapperService.documentMapper().mappingSource());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMappingUpdateWithExplicitIndexUnderNestedIsRejected() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), nestedWithSearchableLeaves());
        assertRejected("field [events.code] must not set [index]", () -> merge(mapperService, nestedWithExplicitNumericLeaf()));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMappingUpdateAddingNestedLeafKeepsTheObjectFlag() throws Exception {
        MapperService mapperService = createMapperService(pluggableSettings(), nestedWithSearchableLeaves());

        merge(mapperService, mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("added").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));

        Map<String, Object> stored = storedMapping(mapperService);
        assertNestedNotIndexed(stored, "events");
        assertNoIndexKey(stored, "events.properties.added");
        assertTrue(mapperService.fieldType("events.added").isSearchable());
        assertFalse(mapperService.getObjectMapper("events").nested().isIndexed());
    }

    // ------------------------------------------------------------------
    // The nested `index` parameter itself
    // ------------------------------------------------------------------

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitIndexOnNestedIsRejectedOnPluggableIndex() throws Exception {
        for (boolean value : new boolean[] { true, false }) {
            String expected = value ? "nested object [events] must not set [index]" : "[index] on [events] is set by the server";
            assertRejected(expected, () -> createMapperService(pluggableSettings(), mapping(b -> {
                b.startObject("events");
                {
                    b.field("type", "nested").field("index", value);
                    b.startObject("properties");
                    b.startObject("name").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            })));
        }
    }

    public void testExplicitIndexOnNestedIsRejectedOnVanillaIndex() throws Exception {
        for (boolean value : new boolean[] { true, false }) {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
                b.startObject("events");
                {
                    b.field("type", "nested").field("index", value);
                    b.startObject("properties");
                    b.startObject("name").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            })));
            assertTrue(
                e.getMessage(),
                e.getMessage().contains("[index] on nested object [events] is only supported with a pluggable data format")
            );
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIndexIsNotAParameterOfPlainObjects() throws Exception {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("obj");
            {
                b.field("index", false);
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        })));
        assertTrue(e.getMessage(), e.getMessage().contains("unsupported parameters"));
    }

    public void testNestedIndexCannotBeUpdated() {
        ObjectMapper.Nested stored = ObjectMapper.Nested.newNested(
            new Explicit<>(false, false),
            new Explicit<>(false, false),
            new Explicit<>(false, true)
        );
        ObjectMapper.Nested incoming = ObjectMapper.Nested.newNested();

        MapperException e = expectThrows(MapperException.class, () -> stored.merge(incoming, MergeReason.MAPPING_UPDATE));
        assertTrue(e.getMessage(), e.getMessage().contains("[index] parameter can't be updated"));
        ObjectMapper.Nested composed = ObjectMapper.Nested.newNested();
        composed.merge(stored, MergeReason.INDEX_TEMPLATE);
        assertFalse(composed.isIndexed());
    }

    // ------------------------------------------------------------------
    // Boundaries
    // ------------------------------------------------------------------

    public void testVanillaIndexIsUntouched() throws Exception {
        MapperService mapperService = createMapperService(nestedWithSearchableLeaves());
        Map<String, Object> stored = storedMapping(mapperService);

        assertFalse("no pluggable format: nothing may be rewritten", field(stored, "events").containsKey("index"));
        assertTrue(mapperService.fieldType("events.name").isSearchable());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDeclinedSearchOnMapperThatCannotRecordItFailsTheMapping() throws Exception {
        // geo_shape has no search capability, so requestedCapabilities() throws UOE. Must surface as a 400.
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("where").field("type", "geo_shape").endObject();
        })));
        assertNotNull(e.getMessage());
    }
}
