/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.mapper.murmur3;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.plugin.mapper.MapperMurmur3Plugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Murmur3FieldMapperTests extends MapperTestCase {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("value");
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperMurmur3Plugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "murmur3");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("store", b -> b.field("store", true));
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.field("field", "value")));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertNotNull(fields);
        assertEquals(Arrays.toString(fields), 1, fields.length);
        IndexableField field = fields[0];
        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatMurmur3() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(pluggableSettings, fieldMapping(this::minimalMapping));
        TestDocumentInput docInput = new TestDocumentInput();
        mapper.parse(source(b -> b.field("field", "test_value")), docInput);

        boolean found = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
        assertTrue("Expected murmur3 field captured with hash value", found);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatMurmur3NullSkipped() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(pluggableSettings, fieldMapping(this::minimalMapping));
        TestDocumentInput docInput = new TestDocumentInput();
        mapper.parse(source(b -> b.nullField("field")), docInput);

        boolean hasField = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
        assertFalse("Expected no captured field for null value", hasField);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggablePathEquivalenceWithLucenePath() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();

        // Scenario 1: murmur3 value
        {
            DocumentMapper luceneMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
            ParsedDocument luceneDoc = luceneMapper.parse(source(b -> b.field("field", "test_value")));
            IndexableField[] luceneFields = luceneDoc.rootDoc().getFields("field");

            DocumentMapper pluggableMapper = createDocumentMapper(pluggableSettings, fieldMapping(this::minimalMapping));
            TestDocumentInput docInput = new TestDocumentInput();
            pluggableMapper.parse(source(b -> b.field("field", "test_value")), docInput);

            assertTrue("Lucene path should produce field 'field'", luceneFields.length > 0);
            boolean pluggableFound = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
            assertTrue("Pluggable path should capture field 'field'", pluggableFound);
        }

        // Scenario 2: null value — no field produced
        {
            DocumentMapper luceneMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
            ParsedDocument luceneDoc = luceneMapper.parse(source(b -> b.nullField("field")));
            IndexableField[] luceneFields = luceneDoc.rootDoc().getFields("field");

            DocumentMapper pluggableMapper = createDocumentMapper(pluggableSettings, fieldMapping(this::minimalMapping));
            TestDocumentInput docInput = new TestDocumentInput();
            pluggableMapper.parse(source(b -> b.nullField("field")), docInput);

            assertEquals("Lucene path should produce no field 'field'", 0, luceneFields.length);
            boolean pluggableHasField = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
            assertFalse("Pluggable path should produce no field 'field'", pluggableHasField);
        }
    }

    public void testHashCalculation() throws Exception {
        String testValue = "test_value";
        BytesRef bytes = new BytesRef(testValue);
        long hash = MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, new MurmurHash3.Hash128()).h1;

        // Verify hash is calculated (non-zero for non-empty input)
        assertNotEquals("Hash should not be zero for non-empty input", 0L, hash);

        // Verify consistent hashing
        BytesRef bytes2 = new BytesRef(testValue);
        long hash2 = MurmurHash3.hash128(bytes2.bytes, bytes2.offset, bytes2.length, 0, new MurmurHash3.Hash128()).h1;
        assertEquals("Hash should be consistent for same input", hash, hash2);
    }

    private static class TestDocumentInput implements DocumentInput<Object> {
        private final List<Map.Entry<MappedFieldType, Object>> capturedFields = new ArrayList<>();

        @Override
        public Object getFinalInput() {
            return null;
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            capturedFields.add(Map.entry(fieldType, value));
        }

        @Override
        public void setRowId(String rowIdFieldName, long rowId) {}

        @Override
        public void close() {}

        public List<Map.Entry<MappedFieldType, Object>> getCapturedFields() {
            return capturedFields;
        }
    }
}
