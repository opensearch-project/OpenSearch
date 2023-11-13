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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MatchOnlyTextFieldMapperTests extends TextFieldMapperTests {

    @BeforeClass
    public static void beforeClass() {
        textFieldName = "match_only_text";
    }

    @Override
    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(fieldMapping(this::minimalMapping).toString(), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals("1234", fields[0].stringValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
    }

    @Override
    public void testEnableStore() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", textFieldName).field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertTrue(fields[0].fieldType().stored());
    }

    @Override
    public void testIndexOptions() throws IOException {
        Map<String, IndexOptions> supportedOptions = new HashMap<>();
        supportedOptions.put("docs", IndexOptions.DOCS);

        Map<String, IndexOptions> unSupportedOptions = new HashMap<>();
        unSupportedOptions.put("freqs", IndexOptions.DOCS_AND_FREQS);
        unSupportedOptions.put("positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        unSupportedOptions.put("offsets", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        for (String option : supportedOptions.keySet()) {
            XContentBuilder mapping = MediaTypeRegistry.JSON.contentBuilder().startObject().startObject("_doc").startObject("properties");
            mapping.startObject(option).field("type", textFieldName).field("index_options", option).endObject();
            mapping.endObject().endObject().endObject();

            DocumentMapper mapper = createDocumentMapper(mapping);
            String serialized = Strings.toString(MediaTypeRegistry.JSON, mapper);
            assertThat(serialized, containsString("\"docs\":{\"type\":\"match_only_text\"}"));

            ParsedDocument doc = mapper.parse(source(b -> { b.field(option, "1234"); }));

            IndexOptions options = supportedOptions.get(option);
            IndexableField[] fields = doc.rootDoc().getFields(option);
            assertEquals(1, fields.length);
            assertEquals(options, fields[0].fieldType().indexOptions());
        }

        for (String option : unSupportedOptions.keySet()) {
            XContentBuilder mapping = MediaTypeRegistry.JSON.contentBuilder().startObject().startObject("_doc").startObject("properties");
            mapping.startObject(option).field("type", textFieldName).field("index_options", option).endObject();
            mapping.endObject().endObject().endObject();
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping));
            assertThat(
                e.getMessage(),
                containsString(
                    "Failed to parse mapping [_doc]: Unknown value [" + option + "] for field [index_options] - accepted values are [docs]"
                )
            );
        }
    }

    @Override
    public void testAnalyzedFieldPositionIncrementWithoutPositions() {
        for (String indexOptions : List.of("docs")) {
            try {
                createDocumentMapper(
                    fieldMapping(
                        b -> b.field("type", textFieldName).field("index_options", indexOptions).field("position_increment_gap", 10)
                    )
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void testBWCSerialization() throws IOException {

    }

    public void testPositionIncrementGap() throws IOException {
        final int positionIncrementGap = randomIntBetween(1, 1000);
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", textFieldName).field("position_increment_gap", positionIncrementGap))
        );
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", new String[] { "a", "b" })));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals("a", fields[0].stringValue());
        assertEquals("b", fields[1].stringValue());

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            TermsEnum terms = getOnlyLeafReader(reader).terms("field").iterator();
            assertTrue(terms.seekExact(new BytesRef("b")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(positionIncrementGap + 1, postings.nextPosition());
        });
    }

    @Override
    public void testSimpleMerge() throws IOException {
        XContentBuilder startingMappingBad = fieldMapping(
            b -> b.field("type", textFieldName).startObject("index_prefixes").endObject().field("index_phrases", true)
        );

        MapperParsingException exc = expectThrows(MapperParsingException.class, () -> createMapperService(startingMappingBad));
        assertThat(
            exc.getMessage(),
            containsString("Failed to parse mapping [_doc]: Cannot set index_phrases on field [field] if positions are not enabled")
        );

        XContentBuilder startingMapping = fieldMapping(
            b -> b.field("type", textFieldName).startObject("index_prefixes").endObject().field("index_phrases", false)
        );
        MapperService mapperService = createMapperService(startingMapping);

        XContentBuilder differentPrefix = fieldMapping(
            b -> b.field("type", textFieldName)
                .startObject("index_prefixes")
                .field("min_chars", "3")
                .endObject()
                .field("index_phrases", false)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, differentPrefix));
        assertThat(e.getMessage(), containsString("Cannot update parameter [index_prefixes]"));

        XContentBuilder newField = mapping(b -> {
            b.startObject("field")
                .field("type", textFieldName)
                .startObject("index_prefixes")
                .endObject()
                .field("index_phrases", false)
                .endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
        });
        merge(mapperService, newField);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(TextFieldMapper.class));
        assertThat(mapperService.documentMapper().mappers().getMapper("other_field"), instanceOf(KeywordFieldMapper.class));
    }
}
