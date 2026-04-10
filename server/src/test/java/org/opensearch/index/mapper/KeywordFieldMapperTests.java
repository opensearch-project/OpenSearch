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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockLowerCaseFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.CustomAnalyzer;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.LowercaseNormalizer;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.PreConfiguredTokenFilter;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.termvectors.TermVectorsService;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.opensearch.index.mapper.KeywordFieldMapper.normalizeValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class KeywordFieldMapperTests extends MapperTestCase {

    private static final String FIELD_NAME = "field";

    /**
     * Creates a copy of the lowercase token filter which we use for testing merge errors.
     */
    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {
        @Override
        public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
            return singletonList(PreConfiguredTokenFilter.singleton("mock_other_lowercase", true, MockLowerCaseFilter::new));
        }

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return singletonMap(
                "keyword",
                (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                    name,
                    () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                )
            );
        }

    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("value");
    }

    public final void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
            if (randomBoolean()) {
                b.field("norms", false);
            }
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public final void testExistsQueryDocValuesDisabledWithNorms() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
            b.field("norms", true);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MockAnalysisPlugin());
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.of(
                "lowercase",
                new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()),
                "other_lowercase",
                new NamedAnalyzer("other_lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())
            ),
            singletonMap(
                "lowercase",
                new NamedAnalyzer(
                    "lowercase",
                    AnalyzerScope.INDEX,
                    new CustomAnalyzer(
                        TokenizerFactory.newFactory("lowercase", WhitespaceTokenizer::new),
                        new CharFilterFactory[0],
                        new TokenFilterFactory[] { new TokenFilterFactory() {

                            @Override
                            public String name() {
                                return "lowercase";
                            }

                            @Override
                            public TokenStream create(TokenStream tokenStream) {
                                return new LowerCaseFilter(tokenStream);
                            }
                        } }
                    )
                )
            )
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "keyword");
    }

    @Override
    protected void assertParseMaximalWarnings() {
        assertWarnings("Parameter [boost] on field [field] is deprecated and will be removed in 3.0");
    }

    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("index_options", b -> b.field("index_options", "freqs"));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "foo"));
        checker.registerConflictCheck("similarity", b -> b.field("similarity", "boolean"));
        checker.registerConflictCheck("normalizer", b -> b.field("normalizer", "lowercase"));

        checker.registerUpdateCheck(b -> b.field("eager_global_ordinals", true), m -> assertTrue(m.fieldType().eagerGlobalOrdinals()));
        checker.registerUpdateCheck(b -> b.field("ignore_above", 256), m -> assertEquals(256, ((KeywordFieldMapper) m).ignoreAbove()));
        checker.registerUpdateCheck(
            b -> b.field("split_queries_on_whitespace", true),
            m -> assertEquals("_whitespace", m.fieldType().getTextSearchInfo().getSearchAnalyzer().name())
        );

        // norms can be set from true to false, but not vice versa
        checker.registerConflictCheck("norms", b -> b.field("norms", true));
        checker.registerUpdateCheck(b -> {
            b.field("type", "keyword");
            b.field("norms", true);
        }, b -> {
            b.field("type", "keyword");
            b.field("norms", false);
        }, m -> assertFalse(m.fieldType().getTextSearchInfo().hasNorms()));

        checker.registerUpdateCheck(b -> b.field("boost", 2.0), m -> assertEquals(m.fieldType().boost(), 2.0, 0));
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping.toString(), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals(new BytesRef("1234"), fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(new BytesRef("1234"), fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());

        // used by TermVectorsService
        assertArrayEquals(new String[] { "1234" }, TermVectorsService.getValues(doc.rootDoc().getFields("field")));
    }

    public void testIgnoreAbove() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("ignore_above", 5)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "elk")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        doc = mapper.parse(source(b -> b.field("field", "opensearch")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("null_value", "uri")));
        doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        doc = mapper.parse(source(b -> b.nullField("field")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("uri"), fields[0].binaryValue());
    }

    public void testEnableStore() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertTrue(fields[0].fieldType().stored());
    }

    public void testDisableIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.NONE, fields[0].fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fields[0].fieldType().docValuesType());
    }

    public void testDisableDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
    }

    public void testIndexOptions() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("index_options", "freqs")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(IndexOptions.DOCS_AND_FREQS, fields[0].fieldType().indexOptions());

        for (String indexOptions : Arrays.asList("positions", "offsets")) {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "keyword").field("index_options", indexOptions)))
            );
            assertThat(
                e.getMessage(),
                containsString("Unknown value [" + indexOptions + "] for field [index_options] - accepted values are [docs, freqs]")
            );
        }
    }

    public void testBoost() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("boost", 2f)));
        assertThat(mapperService.fieldType("field").boost(), equalTo(2f));
        assertWarnings("Parameter [boost] on field [field] is deprecated and will be removed in 3.0");
    }

    public void testEnableNorms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "keyword").field("doc_values", false).field("norms", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertFalse(fields[0].fieldType().omitNorms());

        IndexableField[] fieldNamesFields = doc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(0, fieldNamesFields.length);
    }

    public void testConfigureSimilarity() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("similarity", "boolean")));
        MappedFieldType ft = mapperService.documentMapper().fieldTypes().get("field");
        assertEquals("boolean", ft.getTextSearchInfo().getSimilarity().name());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "keyword").field("similarity", "BM25")))
        );
        assertThat(e.getMessage(), containsString("Cannot update parameter [similarity] from [boolean] to [BM25]"));
    }

    public void testNormalizer() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("normalizer", "lowercase")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "AbC")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals(new BytesRef("abc"), fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(new BytesRef("abc"), fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
    }

    public void testParsesKeywordNestedEmptyObjectStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> defaultMapper.parse(source(b -> b.startObject("field").endObject()))
        );
        assertEquals(
            "failed to parse field [field] of type [keyword] in document with id '1'. " + "Preview of field's value: '{}'",
            ex.getMessage()
        );
    }

    public void testParsesKeywordNestedListStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject();
                {
                    b.startArray("array_name").value("inner_field_first").value("inner_field_second").endArray();
                }
                b.endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "failed to parse field [field] of type [keyword] in document with id '1'. "
                + "Preview of field's value: '{array_name=[inner_field_first, inner_field_second]}'",
            ex.getMessage()
        );
    }

    public void testParsesKeywordNullStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> defaultMapper.parse(source(b -> b.startObject("field").nullField("field_name").endObject()))
        );
        assertEquals(
            "failed to parse field [field] of type [keyword] in document with id '1'. " + "Preview of field's value: '{field_name=null}'",
            e.getMessage()
        );
    }

    public void testUpdateNormalizer() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("normalizer", "lowercase")));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "keyword").field("normalizer", "other_lowercase")))
        );
        assertEquals(
            "Mapper for [field] conflicts with existing mapper:\n"
                + "\tCannot update parameter [normalizer] from [lowercase] to [other_lowercase]",
            e.getMessage()
        );
    }

    public void testSplitQueriesOnWhitespace() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "keyword").endObject();
            b.startObject("field_with_normalizer");
            {
                b.field("type", "keyword");
                b.field("normalizer", "lowercase");
                b.field("split_queries_on_whitespace", true);
            }
            b.endObject();
        }));

        MappedFieldType fieldType = mapperService.fieldType("field");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        KeywordFieldMapper.KeywordFieldType ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        Analyzer a = ft.getTextSearchInfo().getSearchAnalyzer();
        assertTokenStreamContents(a.tokenStream("", "Hello World"), new String[] { "Hello World" });

        fieldType = mapperService.fieldType("field_with_normalizer");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertThat(ft.getTextSearchInfo().getSearchAnalyzer().name(), equalTo("lowercase"));
        assertTokenStreamContents(
            ft.getTextSearchInfo().getSearchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "hello", "world" }
        );

        mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "keyword").field("split_queries_on_whitespace", true).endObject();
            b.startObject("field_with_normalizer");
            {
                b.field("type", "keyword");
                b.field("normalizer", "lowercase");
                b.field("split_queries_on_whitespace", false);
            }
            b.endObject();
        }));

        fieldType = mapperService.fieldType("field");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertTokenStreamContents(
            ft.getTextSearchInfo().getSearchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "Hello", "World" }
        );

        fieldType = mapperService.fieldType("field_with_normalizer");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertThat(ft.getTextSearchInfo().getSearchAnalyzer().name(), equalTo("lowercase"));
        assertTokenStreamContents(
            ft.getTextSearchInfo().getSearchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "hello world" }
        );
    }

    public void testPossibleToDeriveSource_WhenCopyToPresent() throws IOException {
        FieldMapper.CopyTo copyTo = new FieldMapper.CopyTo.Builder().add("copy_to_field").build();
        KeywordFieldMapper mapper = getMapper(copyTo, Integer.MAX_VALUE, "default", true, false);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenIgnoreAbovePresent() throws IOException {
        KeywordFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), 100, "default", true, false);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenNormalizerPresent() throws IOException {
        KeywordFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), 100, "lowercase", true, false);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenDocValuesAndStoredDisabled() throws IOException {
        KeywordFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), Integer.MAX_VALUE, "default", false, false);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testDerivedValueFetching_DocValues() throws IOException {
        try (Directory directory = newDirectory()) {
            KeywordFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), Integer.MAX_VALUE, "default", true, false);
            String value = "keyword_value";
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(createDocument(mapper, value, true));
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                String source = builder.toString();
                assertEquals("{\"" + FIELD_NAME + "\":" + "\"" + value + "\"" + "}", source);
            }
        }
    }

    public void testDerivedValueFetching_StoredField() throws IOException {
        try (Directory directory = newDirectory()) {
            KeywordFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), Integer.MAX_VALUE, "default", false, true);
            String value = "keyword_value";
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(createDocument(mapper, value, false));
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                String source = builder.toString();
                assertEquals("{\"" + FIELD_NAME + "\":" + "\"" + value + "\"" + "}", source);
            }
        }
    }

    private KeywordFieldMapper getMapper(
        FieldMapper.CopyTo copyTo,
        int ignoreAbove,
        String normalizerName,
        boolean hasDocValues,
        boolean isStored
    ) throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "keyword")
                    .field("store", isStored)
                    .field("doc_values", hasDocValues)
                    .field("normalizer", normalizerName)
                    .field("ignore_above", ignoreAbove)
            )
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper(FIELD_NAME);
        mapper.copyTo = copyTo;
        return mapper;
    }

    /**
     * Helper method to create a document with both doc values and stored fields
     */
    private Document createDocument(KeywordFieldMapper mapper, String value, boolean hasDocValues) throws IOException {
        Document doc = new Document();
        FieldType fieldType = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
        fieldType.setStored(!hasDocValues);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        NamedAnalyzer normalizer = mapper.fieldType().normalizer();
        value = normalizeValue(normalizer, FIELD_NAME, value);
        final BytesRef binaryValue = new BytesRef(value);
        if (hasDocValues) {
            doc.add(new SortedSetDocValuesField(FIELD_NAME, binaryValue));
        } else {
            doc.add(new KeywordFieldMapper.KeywordField(FIELD_NAME, binaryValue, fieldType));
        }
        return doc;
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatDefaultKeyword() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.field("field", "test_value")), docInput);

        boolean found = docInput.getCapturedFields()
            .stream()
            .anyMatch(e -> e.getKey().name().equals("field") && e.getValue().equals("test_value"));
        assertTrue("Expected keyword field captured with value 'test_value'", found);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatNullValueSkipped() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.nullField("field")), docInput);

        boolean hasField = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
        assertFalse("Expected no captured field for null value", hasField);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatNullValueConfigured() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("null_value", "default_val").endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.nullField("field")), docInput);

        boolean found = docInput.getCapturedFields()
            .stream()
            .anyMatch(e -> e.getKey().name().equals("field") && e.getValue().equals("default_val"));
        assertTrue("Expected keyword field captured with null_value 'default_val'", found);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatIgnoreAbove() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("ignore_above", 5).endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.field("field", "opensearch")), docInput);

        boolean hasField = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
        assertFalse("Expected no captured field when value exceeds ignore_above", hasField);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatIgnoreAboveWithinLimit() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("ignore_above", 5).endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.field("field", "elk")), docInput);

        boolean found = docInput.getCapturedFields()
            .stream()
            .anyMatch(e -> e.getKey().name().equals("field") && e.getValue().equals("elk"));
        assertTrue("Expected keyword field captured with value 'elk' within ignore_above limit", found);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatWithNormalizer() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("normalizer", "lowercase").endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.field("field", "AbC")), docInput);

        boolean found = docInput.getCapturedFields()
            .stream()
            .anyMatch(e -> e.getKey().name().equals("field") && e.getValue().equals("abc"));
        assertTrue("Expected keyword field captured with normalized value 'abc'", found);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatWithExternalValue() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(pluggableSettings, mapping(b -> {
            b.startObject("text_field");
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("kw").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.field("text_field", "external_keyword")), docInput);

        boolean found = docInput.getCapturedFields()
            .stream()
            .anyMatch(e -> e.getKey().name().equals("text_field.kw") && e.getValue().equals("external_keyword"));
        assertTrue("Expected keyword sub-field captured with external value 'external_keyword'", found);
    }

    public void testDefaultsDoNotUseDocumentInput() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("1234"), fields[0].binaryValue());
        assertEquals(new BytesRef("1234"), fields[1].binaryValue());
    }

    /**
     * Cross-path equivalence test: verifies that the pluggable DocumentInput path
     * captures the same field values as the Lucene Document path for all common
     * keyword scenarios (default, null_value, ignore_above, normalizer, multi-field).
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggablePathEquivalenceWithLucenePath() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();

        // Scenario 1: default keyword
        assertLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").endObject()),
            b -> b.field("field", "1234"),
            "field",
            "1234"
        );

        // Scenario 2: null value — no field produced
        assertLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").endObject()),
            b -> b.nullField("field"),
            "field",
            null
        );

        // Scenario 3: null_value configured — substitution kicks in
        assertLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("null_value", "uri").endObject()),
            b -> b.nullField("field"),
            "field",
            "uri"
        );

        // Scenario 4: ignore_above — value exceeds limit, no field produced
        assertLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("ignore_above", 5).endObject()),
            b -> b.field("field", "opensearch"),
            "field",
            null
        );

        // Scenario 5: ignore_above — value within limit
        assertLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("ignore_above", 5).endObject()),
            b -> b.field("field", "elk"),
            "field",
            "elk"
        );

        // Scenario 6: normalizer
        assertLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("normalizer", "lowercase").endObject()),
            b -> b.field("field", "AbC"),
            "field",
            "abc"
        );
    }

    /**
     * Parses the same source through both the Lucene path and the pluggable DocumentInput path,
     * then asserts they agree on the produced field value (or absence thereof).
     *
     * @param expectedValue the expected value, or null if no field should be produced
     */
    private void assertLuceneAndPluggablePathsEquivalent(
        Settings pluggableSettings,
        XContentBuilder mappingBuilder,
        CheckedConsumer<XContentBuilder, IOException> sourceBuilder,
        String fieldName,
        String expectedValue
    ) throws IOException {
        // Lucene path (default, no pluggable setting)
        DocumentMapper luceneMapper = createDocumentMapper(mappingBuilder);
        ParsedDocument luceneDoc = luceneMapper.parse(source(sourceBuilder));
        IndexableField[] luceneFields = luceneDoc.rootDoc().getFields(fieldName);

        // Pluggable path
        DocumentMapper pluggableMapper = createDocumentMapper(pluggableSettings, mappingBuilder);
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        pluggableMapper.parse(source(sourceBuilder), docInput);

        if (expectedValue == null) {
            // Both paths should produce no field
            assertEquals("Lucene path should produce no field for '" + fieldName + "'", 0, luceneFields.length);
            boolean pluggableHasField = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals(fieldName));
            assertFalse("Pluggable path should produce no field for '" + fieldName + "'", pluggableHasField);
        } else {
            // Lucene path should have produced the field with the expected value
            assertTrue("Lucene path should produce field '" + fieldName + "'", luceneFields.length > 0);
            assertEquals(new BytesRef(expectedValue), luceneFields[0].binaryValue());

            // Pluggable path should capture the same value
            boolean pluggableFound = docInput.getCapturedFields()
                .stream()
                .anyMatch(e -> e.getKey().name().equals(fieldName) && e.getValue().equals(expectedValue));
            assertTrue("Pluggable path should capture field '" + fieldName + "' with value '" + expectedValue + "'", pluggableFound);
        }
    }
}
