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

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class MultiValueFieldMapperTests extends MapperServiceTestCase {

    private record FieldCase(String type, Object first, Object second) {
    }

    private static List<FieldCase> supportedScalarFields() {
        return List.of(
            new FieldCase("keyword", "prod", "error"),
            new FieldCase("text", "first message", "second message"),
            new FieldCase("match_only_text", "first message", "second message"),
            new FieldCase("byte", 1, 2),
            new FieldCase("short", 10, 20),
            new FieldCase("integer", 100, 200),
            new FieldCase("long", 1_000L, 2_000L),
            new FieldCase("half_float", 1.5f, 2.5f),
            new FieldCase("float", 1.25f, 2.25f),
            new FieldCase("double", 1.125d, 2.125d),
            new FieldCase("unsigned_long", 10L, 20L),
            new FieldCase("boolean", true, false),
            new FieldCase("date", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
            new FieldCase("date_nanos", "2026-01-01T00:00:00.000000001Z", "2026-01-01T00:00:00.000000002Z"),
            new FieldCase("ip", "10.0.0.1", "10.0.0.2"),
            new FieldCase("binary", "AQI=", "AwQ=")
        );
    }

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
    }

    private DocumentMapper mapper(String type) throws IOException {
        return mapper(type, null);
    }

    private DocumentMapper mapper(String type, Boolean multiValue) throws IOException {
        return createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("field").field("type", type);
            if (multiValue != null) {
                b.field("multi_value", multiValue);
            }
            if ("binary".equals(type)) {
                b.field("store", true);
            }
            b.endObject();
        }));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSecondValuePromotesEverySupportedScalarFamily() throws IOException {
        for (FieldCase fieldCase : supportedScalarFields()) {
            DocumentMapper mapper = mapper(fieldCase.type());
            CapturingDocumentInput input = new CapturingDocumentInput();
            ParsedDocument parsed = mapper.parse(
                source(b -> b.startArray("field").value(fieldCase.first()).value(fieldCase.second()).endArray()),
                input
            );

            assertEquals(fieldCase.type(), 2L, input.getFieldCount("field"));
            assertNotNull(fieldCase.type(), parsed.dynamicMappingsUpdate());
            Mapper update = parsed.dynamicMappingsUpdate().root().getMapper("field");
            assertThat(fieldCase.type(), update, instanceOf(ParametrizedFieldMapper.class));
            assertTrue(fieldCase.type(), ((FieldMapper) update).fieldType().isMultiValued());
            assertTrue(fieldCase.type(), ((FieldMapper) update).fieldType().isMultiValueSupported());
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSingletonArrayStaysScalarEverySupportedScalarFamily() throws IOException {
        for (FieldCase fieldCase : supportedScalarFields()) {
            DocumentMapper mapper = mapper(fieldCase.type());
            CapturingDocumentInput input = new CapturingDocumentInput();
            ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value(fieldCase.first()).endArray()), input);

            assertEquals(fieldCase.type(), 1L, input.getFieldCount("field"));
            assertNull(fieldCase.type(), parsed.dynamicMappingsUpdate());
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIgnoredNullElementDoesNotPromoteNumericField() throws IOException {
        DocumentMapper mapper = mapper("integer");
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value(1).nullValue().endArray()), input);

        assertEquals(1L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testEmptyArrayPromotesEverySupportedScalarFamily() throws IOException {
        for (FieldCase fieldCase : supportedScalarFields()) {
            DocumentMapper mapper = mapper(fieldCase.type());
            ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), new CapturingDocumentInput());

            assertNotNull(fieldCase.type(), parsed.dynamicMappingsUpdate());
            FieldMapper update = (FieldMapper) parsed.dynamicMappingsUpdate().root().getMapper("field");
            assertTrue(fieldCase.type(), update.fieldType().isMultiValued());
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitMultiValueMappingEverySupportedScalarFamily() throws IOException {
        for (FieldCase fieldCase : supportedScalarFields()) {
            DocumentMapper mapper = createDocumentMapper(pluggableSettings(), mapping(b -> {
                b.startObject("field").field("type", fieldCase.type()).field("multi_value", true);
                if ("binary".equals(fieldCase.type())) {
                    b.field("store", true);
                }
                b.endObject();
            }));
            FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
            assertTrue(fieldCase.type(), fieldMapper.fieldType().isMultiValued());
            assertTrue(fieldCase.type(), fieldMapper.fieldType().isMultiValueSupported());
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseLocksEverySupportedScalarFamily() throws IOException {
        for (FieldCase fieldCase : supportedScalarFields()) {
            DocumentMapper mapper = mapper(fieldCase.type(), false);
            FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
            assertEquals(fieldCase.type(), MappedFieldType.MultiValueState.SCALAR, fieldMapper.fieldType().multiValueState());
            assertThat(fieldCase.type(), mapper.mappingSource().string(), containsString("\"multi_value\":false"));

            ParsedDocument singleton = mapper.parse(
                source(b -> b.startArray("field").value(fieldCase.first()).endArray()),
                new CapturingDocumentInput()
            );
            assertNull(fieldCase.type(), singleton.dynamicMappingsUpdate());

            MapperParsingException error = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(
                    source(b -> b.startArray("field").value(fieldCase.first()).value(fieldCase.second()).endArray()),
                    new CapturingDocumentInput()
                )
            );
            assertNotNull(fieldCase.type(), error.getCause());
            assertThat(fieldCase.type(), error.getCause().getMessage(), containsString("locked scalar by [multi_value: false]"));
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseRejectsEmptyArray() throws IOException {
        DocumentMapper mapper = mapper("integer", false);
        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").endArray()), new CapturingDocumentInput())
        );
        assertThat(error.getMessage(), containsString("locked scalar by [multi_value: false]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOmittedParameterRemainsAutoAndIsNotSerialized() throws IOException {
        DocumentMapper mapper = mapper("integer");
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertEquals(MappedFieldType.MultiValueState.AUTO, fieldMapper.fieldType().multiValueState());
        assertThat(mapper.mappingSource().string(), not(containsString("multi_value")));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseCannotBeUpdatedToTrueAndUnspecifiedUpdatePreservesLock() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "integer").field("multi_value", false).endObject())
        );

        merge(mapperService, mapping(b -> b.startObject("field").field("type", "integer").endObject()));
        assertEquals(MappedFieldType.MultiValueState.SCALAR, mapperService.fieldType("field").multiValueState());

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject("field").field("type", "integer").field("multi_value", true).endObject()))
        );
        assertThat(error.getMessage(), containsString("Cannot update parameter [multi_value] from [false] to [true]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIndexSortFieldCanPromote() throws IOException {
        Settings settings = Settings.builder()
            .put(pluggableSettings())
            .putList("index.sort.field", "field")
            .putList("index.sort.order", "asc")
            .build();
        DocumentMapper mapper = createDocumentMapper(settings, mapping(b -> b.startObject("field").field("type", "keyword").endObject()));

        ParsedDocument parsed = mapper.parse(source(b -> b.array("field", "z", "a")), new CapturingDocumentInput());
        assertNotNull(parsed.dynamicMappingsUpdate());
        FieldMapper update = (FieldMapper) parsed.dynamicMappingsUpdate().root().getMapper("field");
        assertEquals(MappedFieldType.MultiValueState.LIST, update.fieldType().multiValueState());
    }
}
