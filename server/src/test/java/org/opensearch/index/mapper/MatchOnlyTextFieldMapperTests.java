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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.analysis.MockSynonymAnalyzer;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.SourceFieldMatchQuery;
import org.opensearch.index.search.MatchQuery;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

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

        Map<String, IndexOptions> unsupportedOptions = new HashMap<>();
        unsupportedOptions.put("freqs", IndexOptions.DOCS_AND_FREQS);
        unsupportedOptions.put("positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        unsupportedOptions.put("offsets", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

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

        for (String option : unsupportedOptions.keySet()) {
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
    public void testBWCSerialization() throws IOException {}

    @Override
    public void testPositionIncrementGap() throws IOException {}

    @Override
    public void testDefaultPositionIncrementGap() throws IOException {}

    @Override
    public void testIndexPrefixMapping() throws IOException {}

    @Override
    public void testIndexPrefixIndexTypes() throws IOException {}

    @Override
    public void testFastPhrasePrefixes() throws IOException {}

    @Override
    public void testFastPhraseMapping() throws IOException {}

    @Override
    public void testSimpleMerge() throws IOException {}

    public void testPhraseQuery() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", textFieldName).field("analyzer", "my_stop_analyzer").endObject();
            // "standard" will be replaced with MockSynonymAnalyzer
            b.startObject("synfield").field("type", textFieldName).field("analyzer", "standard").endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);

        Query q = new MatchPhraseQueryBuilder("field", "two words").toQuery(queryShardContext);
        Query expectedQuery = new SourceFieldMatchQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("field", "words")), BooleanClause.Occur.FILTER)
                .build(),
            new PhraseQuery("field", "two", "words"),
            mapperService.fieldType("field"),
            queryShardContext
        );

        assertThat(q, is(expectedQuery));
        Query q4 = new MatchPhraseQueryBuilder("field", "singleton").toQuery(queryShardContext);
        assertThat(q4, is(new TermQuery(new Term("field", "singleton"))));

        Query q2 = new MatchPhraseQueryBuilder("field", "three words here").toQuery(queryShardContext);
        expectedQuery = new SourceFieldMatchQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "three")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("field", "words")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("field", "here")), BooleanClause.Occur.FILTER)
                .build(),
            new PhraseQuery("field", "three", "words", "here"),
            mapperService.fieldType("field"),
            queryShardContext
        );
        assertThat(q2, is(expectedQuery));

        Query q3 = new MatchPhraseQueryBuilder("field", "two words").slop(2).toQuery(queryShardContext);
        expectedQuery = new SourceFieldMatchQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("field", "words")), BooleanClause.Occur.FILTER)
                .build(),
            new PhraseQuery(2, "field", "two", "words"),
            mapperService.fieldType("field"),
            queryShardContext
        );
        assertThat(q3, is(expectedQuery));

        Query q5 = new MatchPhraseQueryBuilder("field", "sparkle a stopword").toQuery(queryShardContext);
        expectedQuery = new SourceFieldMatchQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "sparkle")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("field", "stopword")), BooleanClause.Occur.FILTER)
                .build(),
            new PhraseQuery.Builder().add(new Term("field", "sparkle")).add(new Term("field", "stopword"), 2).build(),
            mapperService.fieldType("field"),
            queryShardContext
        );
        assertThat(q5, is(expectedQuery));

        MatchQuery matchQuery = new MatchQuery(queryShardContext);
        matchQuery.setAnalyzer(new MockSynonymAnalyzer());
        Query q6 = matchQuery.parse(MatchQuery.Type.PHRASE, "synfield", "motor dogs");
        expectedQuery = new SourceFieldMatchQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("synfield", "motor")), BooleanClause.Occur.FILTER)
                .add(
                    new BooleanQuery.Builder().add(new TermQuery(new Term("synfield", "dogs")), BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term("synfield", "dog")), BooleanClause.Occur.SHOULD)
                        .build(),
                    BooleanClause.Occur.FILTER
                )
                .build(),
            new MultiPhraseQuery.Builder().add(new Term("synfield", "motor"))
                .add(new Term[] { new Term("synfield", "dogs"), new Term("synfield", "dog") }, 1)
                .build(),
            mapperService.fieldType("synfield"),
            queryShardContext
        );
        assertThat(q6, is(expectedQuery));
    }

    public void testMultiPhraseQuery() {

    }

    public void testPrefixQuery() {

    }
}
