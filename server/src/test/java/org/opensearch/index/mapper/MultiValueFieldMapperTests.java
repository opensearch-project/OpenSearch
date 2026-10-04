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
import java.util.LinkedHashMap;
import java.util.Map;

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
    public void testSecondValuePromotesKeyword() throws IOException {
        DocumentMapper mapper = keywordMapper();
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value("prod").value("error").endArray()), input);

        assertEquals(2L, input.getFieldCount("field"));
        assertNotNull(parsed.dynamicMappingsUpdate());
        Mapper update = parsed.dynamicMappingsUpdate().root().getMapper("field");
        assertThat(update, instanceOf(ParametrizedFieldMapper.class));
        assertTrue(((FieldMapper) update).fieldType().isMultiValued());
        assertTrue(((FieldMapper) update).fieldType().isMultiValueSupported());
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
    public void testEmptyArrayPromotesKeyword() throws IOException {
        DocumentMapper mapper = keywordMapper();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), new CapturingDocumentInput());

        assertNotNull(parsed.dynamicMappingsUpdate());
        FieldMapper update = (FieldMapper) parsed.dynamicMappingsUpdate().root().getMapper("field");
        assertTrue(update.fieldType().isMultiValued());
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
    public void testExplicitFalseRejectsEmptyArray() throws IOException {
        DocumentMapper mapper = keywordMapper(false);
        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").endArray()), new CapturingDocumentInput())
        );
        assertThat(error.getMessage(), containsString("locked scalar by [multi_value: false]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOmittedParameterRemainsAutoAndIsNotSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper();
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertEquals(MappedFieldType.MultiValueState.AUTO, fieldMapper.fieldType().multiValueState());
        assertThat(mapper.mappingSource().string(), not(containsString("multi_value")));
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

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitMultiValueSupportedByScalarParquetMappers() throws IOException {
        for (String type : new String[] {
            "byte",
            "short",
            "integer",
            "long",
            "unsigned_long",
            "half_float",
            "float",
            "double",
            "boolean",
            "date",
            "date_nanos",
            "ip",
            "binary",
            "text",
            "match_only_text" }) {
            DocumentMapper mapper = scalarMapper(type, true, pluggableSettings());
            FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
            assertTrue(type, fieldMapper.fieldType().isMultiValued());
            assertTrue(type, fieldMapper.fieldType().isMultiValueSupported());
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNonKeywordMultiValueIsRejectedOutsidePluggableFormat() {
        for (String type : new String[] { "integer", "date", "boolean", "ip", "binary", "text", "match_only_text" }) {
            MapperParsingException error = expectThrows(
                MapperParsingException.class,
                type,
                () -> scalarMapper(type, true, getIndexSettings())
            );
            assertThat(type, error.getMessage(), containsString("unknown parameter [multi_value]"));
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSecondNumericValuePublishesListMappingUpdate() throws IOException {
        DocumentMapper mapper = scalarMapper("integer", null, pluggableSettings());
        CapturingDocumentInput input = new CapturingDocumentInput();

        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").value(10).value(20).endArray()), input);

        assertEquals(2L, input.getFieldCount("field"));
        assertNotNull(parsed.dynamicMappingsUpdate());
        FieldMapper update = (FieldMapper) parsed.dynamicMappingsUpdate().root().getMapper("field");
        assertEquals(MappedFieldType.MultiValueState.LIST, update.fieldType().multiValueState());
        assertTrue(update.fieldType().isMultiValueSupported());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitScalarNumericRejectsSecondValueAndCannotBeRemapped() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "integer").field("multi_value", false).endObject())
        );
        DocumentMapper mapper = mapperService.documentMapper();

        MapperParsingException parseError = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").value(10).value(20).endArray()), new CapturingDocumentInput())
        );
        assertThat(parseError.getCause().getMessage(), containsString("locked scalar by [multi_value: false]"));

        IllegalArgumentException mergeError = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject("field").field("type", "integer").field("multi_value", true).endObject()))
        );
        assertThat(mergeError.getMessage(), containsString("Cannot update parameter [multi_value] from [false] to [true]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitMultiValueScalarMappersAcceptSecondValueWithoutMappingUpdate() throws IOException {
        Map<String, Object[]> samples = new LinkedHashMap<>();
        samples.put("byte", new Object[] { 1, 2 });
        samples.put("short", new Object[] { 100, 200 });
        samples.put("integer", new Object[] { 1000, 2000 });
        samples.put("long", new Object[] { 10000000000L, 20000000000L });
        samples.put("unsigned_long", new Object[] { 12345678901234567L, 23456789012345678L });
        samples.put("half_float", new Object[] { 1.5, 2.5 });
        samples.put("float", new Object[] { 1.5, 2.5 });
        samples.put("double", new Object[] { 1.25, 2.5 });
        samples.put("boolean", new Object[] { true, false });
        samples.put("date", new Object[] { "2020-10-13T13:00:00Z", "2021-01-01T00:00:00Z" });
        samples.put("date_nanos", new Object[] { "2019-03-24T01:34:46.123456789Z", "2019-03-25T02:00:00.000000001Z" });
        samples.put("ip", new Object[] { "192.168.1.1", "10.0.0.1" });
        samples.put("binary", new Object[] { "YWxpY2U=", "Ym9i" });
        samples.put("text", new Object[] { "hello world", "foo" });
        samples.put("match_only_text", new Object[] { "hello world", "foo" });

        for (Map.Entry<String, Object[]> sample : samples.entrySet()) {
            String type = sample.getKey();
            DocumentMapper mapper = scalarMapper(type, true, pluggableSettings());
            CapturingDocumentInput input = new CapturingDocumentInput();
            ParsedDocument parsed = mapper.parse(source(b -> {
                b.startArray("field");
                for (Object value : sample.getValue()) {
                    b.value(value);
                }
                b.endArray();
            }), input);
            assertEquals(type + " must record both values", 2L, input.getFieldCount("field"));
            assertNull(type + " is already multi_value:true and must not request a mapping update", parsed.dynamicMappingsUpdate());
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitMultiValueScalarMappersSerializeAndReparse() throws IOException {
        for (String type : new String[] {
            "byte",
            "short",
            "integer",
            "long",
            "unsigned_long",
            "half_float",
            "float",
            "double",
            "boolean",
            "date",
            "date_nanos",
            "ip",
            "binary",
            "text",
            "match_only_text" }) {
            DocumentMapper mapper = scalarMapper(type, true, pluggableSettings());
            String serialized = mapper.mappingSource().toString();
            assertThat(type + " must serialize multi_value: " + serialized, serialized, containsString("\"multi_value\":true"));

            MapperService reparsed = createMapperService(pluggableSettings(), mapping(b -> {}));
            merge("_doc", reparsed, serialized);
            FieldMapper fieldMapper = (FieldMapper) reparsed.documentMapper().mappers().getMapper("field");
            assertTrue(type + " must be multi-valued after re-parse", fieldMapper.fieldType().isMultiValued());
        }
    }

    private DocumentMapper scalarMapper(String type, Boolean multiValue, Settings settings) throws IOException {
        return createDocumentMapper(settings, mapping(b -> {
            b.startObject("field").field("type", type);
            if ("binary".equals(type)) {
                b.field("store", true);
            }
            if (multiValue != null) {
                b.field("multi_value", multiValue);
            }
            b.endObject();
        }));
    }
}
