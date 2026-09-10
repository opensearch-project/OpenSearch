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
    public void testNonPluggableIndexRejectsMultiValueParameter() {
        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                getIndexSettings(),
                mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
            )
        );
        assertThat(error.getMessage(), containsString("unknown parameter [multi_value]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOmittedParameterDefaultsToScalarAndIsNotSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper();
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");

        assertFalse(fieldMapper.fieldType().isMultiValued());
        assertThat(mapper.mappingSource().string(), not(containsString("multi_value")));

        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.field("field", "prod")), input);
        assertEquals(1L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOmittedParameterRejectsMultipleValuesWithoutMappingUpdate() throws IOException {
        DocumentMapper mapper = keywordMapper();
        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").value("prod").value("error").endArray()), new CapturingDocumentInput())
        );

        assertThat(
            org.opensearch.ExceptionsHelper.stackTrace(error),
            containsString("declare [multi_value: true] when creating the field mapping")
        );
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testScalarFieldIgnoresEmptyArray() throws IOException {
        DocumentMapper mapper = keywordMapper();
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), input);

        assertEquals(0L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseIsScalarAndSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper(false);
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");

        assertFalse(fieldMapper.fieldType().isMultiValued());
        assertThat(mapper.mappingSource().string(), containsString("\"multi_value\":false"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitTrueIsMultiValuedAndSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper(true);
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");

        assertTrue(fieldMapper.fieldType().isMultiValued());
        assertThat(mapper.mappingSource().string(), containsString("\"multi_value\":true"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitTrueAcceptsScalarAndArrayWithoutMappingUpdates() throws IOException {
        DocumentMapper mapper = keywordMapper(true);

        CapturingDocumentInput scalarInput = new CapturingDocumentInput();
        ParsedDocument scalar = mapper.parse(source(b -> b.field("field", "prod")), scalarInput);
        assertEquals(1L, scalarInput.getFieldCount("field"));
        assertNull(scalar.dynamicMappingsUpdate());

        CapturingDocumentInput arrayInput = new CapturingDocumentInput();
        ParsedDocument array = mapper.parse(source(b -> b.array("field", "prod", "error")), arrayInput);
        assertEquals(2L, arrayInput.getFieldCount("field"));
        assertNull(array.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitTruePreservesEmptyArray() throws IOException {
        DocumentMapper mapper = keywordMapper(true);
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), input);

        Object emptyValue = input.getCapturedFields()
            .stream()
            .filter(entry -> entry.getKey().name().equals("field"))
            .map(java.util.Map.Entry::getValue)
            .findFirst()
            .orElseThrow();
        assertEquals(List.of(), emptyValue);
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMultiValueCannotChangeAfterFieldCreation() throws IOException {
        MapperService scalarService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        IllegalArgumentException scalarToList = expectThrows(
            IllegalArgumentException.class,
            () -> merge(scalarService, mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject()))
        );
        assertThat(scalarToList.getMessage(), containsString("Cannot update parameter [multi_value] from [false] to [true]"));

        MapperService listService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
        );
        merge(listService, mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertTrue(listService.fieldType("field").isMultiValued());

        IllegalArgumentException listToScalar = expectThrows(
            IllegalArgumentException.class,
            () -> merge(listService, mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", false).endObject()))
        );
        assertThat(listToScalar.getMessage(), containsString("Cannot update parameter [multi_value] from [true] to [false]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIndexSortFieldAcceptsArrayWhenDeclaredMultiValue() throws IOException {
        Settings settings = Settings.builder()
            .put(pluggableSettings())
            .putList("index.sort.field", "field")
            .putList("index.sort.order", "asc")
            .build();
        DocumentMapper mapper = createDocumentMapper(
            settings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
        );

        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.array("field", "z", "a")), input);
        assertEquals(2L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }
}
