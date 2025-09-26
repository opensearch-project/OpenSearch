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

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.analysis.Token;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test for {@link TokenCountFieldMapper}.
 */
public class TokenCountFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new MapperExtrasModulePlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "token_count").field("analyzer", "keyword");
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("some words");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("skip_list", b -> b.field("skip_list", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerConflictCheck("enable_position_increments", b -> b.field("enable_position_increments", false));
        checker.registerUpdateCheck(this::minimalMapping, b -> b.field("type", "token_count").field("analyzer", "standard"), m -> {
            TokenCountFieldMapper tcfm = (TokenCountFieldMapper) m;
            assertThat(tcfm.analyzer(), equalTo("standard"));
        });
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        NamedAnalyzer dflt = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer standard = new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer keyword = new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer());
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put("default", dflt);
        analyzers.put("standard", standard);
        analyzers.put("keyword", keyword);
        return new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
    }

    @Override
    protected String typeName() {
        return "token_count";
    }

    /**
     *  When position increments are counted, we're looking to make sure that we:
        - don't count tokens without an increment
        - count normal tokens with one increment
        - count funny tokens with more than one increment
        - count the final token increments on the rare token streams that have them
     */
    public void testCountPositionsWithIncrements() throws IOException {
        Analyzer analyzer = createMockAnalyzer();
        assertThat(TokenCountFieldMapper.countPositions(analyzer, "", "", true), equalTo(7));
    }

    /**
     *  When position increments are not counted (only positions are counted), we're looking to make sure that we:
        - don't count tokens without an increment
        - count normal tokens with one increment
        - count funny tokens with more than one increment as only one
        - don't count the final token increments on the rare token streams that have them
     */
    public void testCountPositionsWithoutIncrements() throws IOException {
        Analyzer analyzer = createMockAnalyzer();
        assertThat(TokenCountFieldMapper.countPositions(analyzer, "", "", false), equalTo(2));
    }

    private Analyzer createMockAnalyzer() {
        Token t1 = new Token();      // Token without an increment
        t1.setPositionIncrement(0);
        Token t2 = new Token();
        t2.setPositionIncrement(1);  // Normal token with one increment
        Token t3 = new Token();
        t2.setPositionIncrement(2);  // Funny token with more than one increment
        int finalTokenIncrement = 4; // Final token increment
        Token[] tokens = new Token[] { t1, t2, t3 };
        Collections.shuffle(Arrays.asList(tokens), random());
        final TokenStream tokenStream = new CannedTokenStream(finalTokenIncrement, 0, tokens);
        // TODO: we have no CannedAnalyzer?
        return new Analyzer() {
            @Override
            public TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new MockTokenizer(), tokenStream);
            }
        };
    }

    public void testParseNullValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField(false);
        ParseContext.Document doc = parseDocument(mapper, createDocument(null));
        assertNull(doc.getField("test.tc"));
    }

    public void testParseEmptyValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField(false);
        ParseContext.Document doc = parseDocument(mapper, createDocument(""));
        assertEquals(0, doc.getField("test.tc").numericValue());
    }

    public void testParseNotNullValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField(false);
        ParseContext.Document doc = parseDocument(mapper, createDocument("three tokens string"));
        assertEquals(3, doc.getField("test.tc").numericValue());

        IndexableField[] fields = doc.getFields("test.tc");
        assertEquals(2, fields.length);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(DocValuesSkipIndexType.NONE, dvField.fieldType().docValuesSkipIndexType());
    }

    public void testParseNotNullValue_withSkiplist() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField(true);
        ParseContext.Document doc = parseDocument(mapper, createDocument("three tokens string"));
        assertEquals(3, doc.getField("test.tc").numericValue());

        IndexableField[] fields = doc.getFields("test.tc");
        assertEquals(2, fields.length);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(DocValuesSkipIndexType.RANGE, dvField.fieldType().docValuesSkipIndexType());
    }

    private DocumentMapper createIndexWithTokenCountField(boolean skiplist) throws IOException {
        return createDocumentMapper(mapping(b -> {
            b.startObject("test");
            {
                b.field("type", "text");
                b.startObject("fields");
                {
                    b.startObject("tc");
                    {
                        b.field("type", "token_count");
                        b.field("analyzer", "standard");
                        if (skiplist) {
                            b.field("skip_list", "true");
                        }
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
    }

    private SourceToParse createDocument(String fieldValue) throws Exception {
        return source(b -> b.field("test", fieldValue));
    }

    private ParseContext.Document parseDocument(DocumentMapper mapper, SourceToParse request) {
        return mapper.parse(request).docs().stream().findFirst().orElseThrow(() -> new IllegalStateException("Test object not parsed"));
    }
}
