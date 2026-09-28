/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.termvectors.TermVectorsService;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class MultiValueFieldMapperTests extends MapperServiceTestCase {

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
    }

    private DocumentMapper keywordMapper() throws IOException {
        return keywordMapper(null);
    }

    private DocumentMapper keywordMapper(Boolean multiValue) throws IOException {
        return createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("field").field("type", "keyword");
            if (multiValue != null) {
                b.field("multi_value", multiValue);
            }
            b.endObject();
        }));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNonPluggableIndexAcceptsMultiValueParameter() throws IOException {
        // The parameter is registered irrespective of storage format: Lucene is inherently
        // multi-valued, so the declaration is inert there but must still parse and round-trip.
        DocumentMapper mapper = createDocumentMapper(
            getIndexSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
        );
        assertThat(mapper.mappingSource().string(), containsString("\"multi_value\":true"));
        assertTrue(((FieldMapper) mapper.mappers().getMapper("field")).fieldType().isMultiValued());

        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value("prod").value("error").endArray()));
        assertNull(parsed.dynamicMappingsUpdate());
        assertArrayEquals(new String[] { "prod", "error" }, TermVectorsService.getValues(parsed.rootDoc().getFields("field")));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSecondValueRejectedWhenAutoPromotionDisabled() throws IOException {
        DocumentMapper mapper = keywordMapper();
        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").value("prod").value("error").endArray()), new CapturingDocumentInput())
        );
        String trace = org.opensearch.ExceptionsHelper.stackTrace(error);
        assertThat(trace, containsString("automatic promotion is disabled"));
        assertThat(trace, containsString(FeatureFlags.PARQUET_MULTI_VALUE_AUTO_PROMOTION_EXPERIMENTAL_FLAG));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSecondValuePromotesKeywordWhenExperimentalFlagEnabled() throws Exception {
        FeatureFlags.TestUtils.with(FeatureFlags.PARQUET_MULTI_VALUE_AUTO_PROMOTION_EXPERIMENTAL_FLAG, () -> {
            DocumentMapper mapper = keywordMapper();
            CapturingDocumentInput input = new CapturingDocumentInput();
            ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value("prod").value("error").endArray()), input);

            assertEquals(2L, input.getFieldCount("field"));
            assertNotNull(parsed.dynamicMappingsUpdate());
            Mapper update = parsed.dynamicMappingsUpdate().root().getMapper("field");
            assertThat(update, instanceOf(ParametrizedFieldMapper.class));
            assertTrue(((FieldMapper) update).fieldType().isMultiValued());
            assertTrue(((FieldMapper) update).fieldType().isMultiValueSupported());
        });
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSingletonArrayStaysScalarKeyword() throws IOException {
        DocumentMapper mapper = keywordMapper();
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value("prod").endArray()), input);

        assertEquals(1L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIgnoredNullElementDoesNotPromoteKeyword() throws IOException {
        DocumentMapper mapper = keywordMapper();
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value("prod").nullValue().endArray()), input);

        assertEquals(1L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testEmptyArrayIsAbsentWhenAutoPromotionDisabled() throws IOException {
        DocumentMapper mapper = keywordMapper();
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), input);

        assertEquals(0L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testEmptyArrayPromotesKeywordWhenExperimentalFlagEnabled() throws Exception {
        FeatureFlags.TestUtils.with(FeatureFlags.PARQUET_MULTI_VALUE_AUTO_PROMOTION_EXPERIMENTAL_FLAG, () -> {
            DocumentMapper mapper = keywordMapper();
            ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), new CapturingDocumentInput());

            assertNotNull(parsed.dynamicMappingsUpdate());
            FieldMapper update = (FieldMapper) parsed.dynamicMappingsUpdate().root().getMapper("field");
            assertTrue(update.fieldType().isMultiValued());
        });
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitMultiValueKeywordMapping() throws IOException {
        DocumentMapper mapper = keywordMapper(true);
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertTrue(fieldMapper.fieldType().isMultiValued());
        assertTrue(fieldMapper.fieldType().isMultiValueSupported());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseLocksKeyword() throws IOException {
        DocumentMapper mapper = keywordMapper(false);
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertEquals(MappedFieldType.MultiValueState.SCALAR, fieldMapper.fieldType().multiValueState());
        assertThat(mapper.mappingSource().string(), containsString("\"multi_value\":false"));

        ParsedDocument singleton = mapper.parse(source(b -> b.startArray("field").value("prod").endArray()), new CapturingDocumentInput());
        assertNull(singleton.dynamicMappingsUpdate());

        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").value("prod").value("error").endArray()), new CapturingDocumentInput())
        );
        assertNotNull(error.getCause());
        assertThat(error.getCause().getMessage(), containsString("locked scalar by [multi_value: false]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseAcceptsEmptyArrayAsNoValue() throws IOException {
        DocumentMapper mapper = keywordMapper(false);
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), input);

        assertEquals(0L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOmittedParameterRemainsAutoAndIsNotSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper();
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertEquals(MappedFieldType.MultiValueState.AUTO, fieldMapper.fieldType().multiValueState());
        assertThat(mapper.mappingSource().string(), not(containsString("multi_value")));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testAutoMappingCannotUpdateToListWhenPromotionDisabled() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject()))
        );
        assertThat(error.getMessage(), containsString("Cannot update parameter [multi_value] from [auto] to [true]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseCannotBeUpdatedToTrueAndUnspecifiedUpdatePreservesLock() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", false).endObject())
        );

        merge(mapperService, mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertEquals(MappedFieldType.MultiValueState.SCALAR, mapperService.fieldType("field").multiValueState());

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject()))
        );
        assertThat(error.getMessage(), containsString("Cannot update parameter [multi_value] from [false] to [true]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitTrueCannotBeUpdatedToFalse() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
        );

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", false).endObject())
            )
        );
        assertThat(error.getMessage(), containsString("Cannot update parameter [multi_value] from [true] to [false]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIndexSortFieldCanPromoteWhenExperimentalFlagEnabled() throws Exception {
        FeatureFlags.TestUtils.with(FeatureFlags.PARQUET_MULTI_VALUE_AUTO_PROMOTION_EXPERIMENTAL_FLAG, () -> {
            Settings settings = Settings.builder()
                .put(pluggableSettings())
                .putList("index.sort.field", "field")
                .putList("index.sort.order", "asc")
                .build();
            DocumentMapper mapper = createDocumentMapper(
                settings,
                mapping(b -> b.startObject("field").field("type", "keyword").endObject())
            );

            ParsedDocument parsed = mapper.parse(source(b -> b.array("field", "z", "a")), new CapturingDocumentInput());
            assertNotNull(parsed.dynamicMappingsUpdate());
            FieldMapper update = (FieldMapper) parsed.dynamicMappingsUpdate().root().getMapper("field");
            assertEquals(MappedFieldType.MultiValueState.LIST, update.fieldType().multiValueState());
        });
    }

    // ---- dynamic mapping: array values infer multi_value: true on first sight ----

    /**
     * Dynamic strings on pluggable-format indices map to {@code text}, which does not expose
     * {@code multi_value} yet, so these tests route strings to {@code keyword} through a dynamic
     * template exactly as a log-style index template would.
     */
    private DocumentMapper dynamicKeywordMapper(Settings settings, Boolean templateMultiValue) throws IOException {
        return createDocumentMapper(settings, topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("strings_as_keywords");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword");
                        if (templateMultiValue != null) {
                            b.field("multi_value", templateMultiValue);
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
    }

    private static FieldMapper dynamicUpdateFor(ParsedDocument parsed, String field) {
        assertNotNull("expected a dynamic mapping update", parsed.dynamicMappingsUpdate());
        Mapper mapper = parsed.dynamicMappingsUpdate().root().getMapper(field);
        assertThat(mapper, instanceOf(FieldMapper.class));
        return (FieldMapper) mapper;
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicArrayInfersMultiValueTrueOnFirstDocument() throws IOException {
        DocumentMapper mapper = dynamicKeywordMapper(pluggableSettings(), null);
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("tags").value("prod").value("error").endArray()), input);

        FieldMapper update = dynamicUpdateFor(parsed, "tags");
        assertEquals("keyword", update.typeName());
        assertEquals(MappedFieldType.MultiValueState.LIST, update.fieldType().multiValueState());
        assertTrue(update.fieldType().isMultiValued());
        assertThat(parsed.dynamicMappingsUpdate().toString(), containsString("\"multi_value\":true"));
        // both elements were accepted into the document input under the LIST field type
        assertEquals(2L, input.getFieldCount("tags"));
        assertTrue(
            input.getCapturedFields().stream().filter(e -> e.getKey().name().equals("tags")).allMatch(e -> e.getKey().isMultiValued())
        );
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicSingletonArrayStillInfersMultiValueTrue() throws IOException {
        DocumentMapper mapper = dynamicKeywordMapper(pluggableSettings(), null);
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("tags").value("prod").endArray()), new CapturingDocumentInput());

        FieldMapper update = dynamicUpdateFor(parsed, "tags");
        assertEquals(MappedFieldType.MultiValueState.LIST, update.fieldType().multiValueState());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicScalarStaysAutoAndIsNotSerialized() throws IOException {
        DocumentMapper mapper = dynamicKeywordMapper(pluggableSettings(), null);
        ParsedDocument parsed = mapper.parse(source(b -> b.field("tags", "prod")), new CapturingDocumentInput());

        FieldMapper update = dynamicUpdateFor(parsed, "tags");
        assertEquals(MappedFieldType.MultiValueState.AUTO, update.fieldType().multiValueState());
        assertThat(parsed.dynamicMappingsUpdate().toString(), not(containsString("multi_value")));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicTemplateExplicitScalarIsHonouredForArray() throws IOException {
        DocumentMapper mapper = dynamicKeywordMapper(pluggableSettings(), false);
        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("tags").value("prod").value("error").endArray()), new CapturingDocumentInput())
        );
        assertThat(org.opensearch.ExceptionsHelper.stackTrace(error), containsString("locked scalar by [multi_value: false]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicTemplateExplicitListIsPreservedForScalar() throws IOException {
        DocumentMapper mapper = dynamicKeywordMapper(pluggableSettings(), true);
        ParsedDocument parsed = mapper.parse(source(b -> b.field("tags", "prod")), new CapturingDocumentInput());

        FieldMapper update = dynamicUpdateFor(parsed, "tags");
        assertEquals(MappedFieldType.MultiValueState.LIST, update.fieldType().multiValueState());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicArrayInferenceIsScopedToTheArrayFieldItself() throws IOException {
        DocumentMapper mapper = dynamicKeywordMapper(pluggableSettings(), null);
        ParsedDocument parsed = mapper.parse(source(b -> {
            b.startArray("items");
            b.startObject().field("name", "a").endObject();
            b.endArray();
        }), new CapturingDocumentInput());

        // `name` is a scalar inside an array of objects; only fields whose own value is an array infer LIST.
        Mapper items = parsed.dynamicMappingsUpdate().root().getMapper("items");
        assertThat(items, instanceOf(ObjectMapper.class));
        FieldMapper name = (FieldMapper) ((ObjectMapper) items).getMapper("name");
        assertEquals(MappedFieldType.MultiValueState.AUTO, name.fieldType().multiValueState());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicArrayLeavesBuildersWithoutMultiValueUntouched() throws IOException {
        // No template: strings map to text, which does not register multi_value on this branch.
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {}));
        ParsedDocument parsed = mapper.parse(
            source(b -> b.startArray("tags").value("prod").value("error").endArray()),
            new CapturingDocumentInput()
        );

        FieldMapper update = dynamicUpdateFor(parsed, "tags");
        assertEquals("text", update.typeName());
        assertEquals(MappedFieldType.MultiValueState.AUTO, update.fieldType().multiValueState());
        assertThat(parsed.dynamicMappingsUpdate().toString(), not(containsString("multi_value")));
    }

    public void testDynamicArrayInferenceIgnoredOnNonPluggableIndex() throws IOException {
        DocumentMapper mapper = dynamicKeywordMapper(getIndexSettings(), null);
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("tags").value("prod").value("error").endArray()));

        FieldMapper update = dynamicUpdateFor(parsed, "tags");
        assertEquals("keyword", update.typeName());
        assertEquals(MappedFieldType.MultiValueState.AUTO, update.fieldType().multiValueState());
        assertThat(parsed.dynamicMappingsUpdate().toString(), not(containsString("multi_value")));
    }
}
