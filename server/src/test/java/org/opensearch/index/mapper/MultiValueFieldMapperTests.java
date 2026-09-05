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
}
