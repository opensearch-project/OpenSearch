/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

public class DataFormatPluginAssignCapabilitiesTests extends OpenSearchTestCase {

    public void testSingleFormatCoversAllCapabilities() {
        MockDataFormat format = new MockDataFormat(
            "keyword-format",
            100L,
            Set.of(
                new FieldTypeCapabilities(
                    "keyword",
                    Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH, FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
                )
            )
        );
        DataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("test_field");

        plugin.assignCapabilities(fieldType, null, null);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = fieldType.getCapabilityMap();
        assertEquals(1, capMap.size());
        assertTrue(capMap.containsKey(format));
        assertEquals(
            Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH, FieldTypeCapabilities.Capability.COLUMNAR_STORAGE),
            capMap.get(format)
        );
    }

    public void testSingleFormatPartialCoverage_Throws() {
        MockDataFormat format = new MockDataFormat(
            "partial-format",
            100L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        DataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("test_field");

        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> plugin.assignCapabilities(fieldType, null, null));
        assertTrue(ex.getMessage().contains("FULL_TEXT_SEARCH"));
    }

    public void testFieldWithNoRequestedCapabilities_EmptyMap() {
        MockDataFormat format = new MockDataFormat(
            "some-format",
            100L,
            Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.POINT_RANGE)))
        );
        DataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            "no_caps",
            NumberFieldMapper.NumberType.INTEGER,
            false,
            false,
            false,
            false,
            true,
            null,
            Map.of()
        );

        plugin.assignCapabilities(fieldType, null, null);

        assertTrue(fieldType.getCapabilityMap().isEmpty());
    }

    public void testFormatDoesNotSupportFieldType_Throws() {
        MockDataFormat format = new MockDataFormat(
            "integer-only-format",
            100L,
            Set.of(
                new FieldTypeCapabilities(
                    "integer",
                    Set.of(FieldTypeCapabilities.Capability.POINT_RANGE, FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
                )
            )
        );
        DataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("test_field");

        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> plugin.assignCapabilities(fieldType, null, null));
        assertTrue(ex.getMessage().contains("cannot cover"));
    }

    public void testFormatSupportsSubsetOnly_Throws() {
        MockDataFormat format = new MockDataFormat(
            "bloom-only-format",
            100L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.BLOOM_FILTER)))
        );
        DataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("test_field");

        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> plugin.assignCapabilities(fieldType, null, null));
        assertTrue(ex.getMessage().contains("FULL_TEXT_SEARCH"));
        assertTrue(ex.getMessage().contains("COLUMNAR_STORAGE"));
    }
}
