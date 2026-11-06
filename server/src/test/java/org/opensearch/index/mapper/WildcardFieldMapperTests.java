/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.CustomAnalyzer;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.LowercaseNormalizer;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.opensearch.index.mapper.FieldTypeTestCase.fetchSourceValue;
import static org.opensearch.index.mapper.KeywordFieldMapper.normalizeValue;
import static org.mockito.Mockito.when;

public class WildcardFieldMapperTests extends MapperTestCase {

    private static final String FIELD_NAME = "field";

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "wildcard");
        b.field("doc_values", false);
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("value");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("normalizer", b -> b.field("normalizer", "lowercase"));
        checker.registerConflictCheck("doc_values", fieldMapping(this::minimalMapping), fieldMapping(xcb -> {
            xcb.field("type", "wildcard");
            xcb.field("doc_values", true);
        }));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "foo"));
        checker.registerUpdateCheck(b -> b.field("ignore_above", 256), m -> assertEquals(256, ((WildcardFieldMapper) m).ignoreAbove()));
    }

    public void testTokenizer() throws IOException {
        List<String> terms = new ArrayList<>();
        try (Tokenizer tokenizer = new WildcardFieldMapper.WildcardFieldTokenizer()) {
            tokenizer.setReader(new StringReader("pickle"));
            tokenizer.reset();
            CharTermAttribute charTermAttribute = tokenizer.getAttribute(CharTermAttribute.class);
            while (tokenizer.incrementToken()) {
                terms.add(charTermAttribute.toString());
            }
        }
        assertEquals(
            List.of(
                WildcardFieldTypeTests.prefixAnchored("p"),
                WildcardFieldTypeTests.prefixAnchored("pi"),
                "pic",
                "ick",
                "ckl",
                "kle",
                WildcardFieldTypeTests.suffixAnchored("le"),
                WildcardFieldTypeTests.suffixAnchored("e")
            ),
            terms
        );
        terms.clear();
        try (Tokenizer tokenizer = new WildcardFieldMapper.WildcardFieldTokenizer()) {
            tokenizer.setReader(new StringReader("a"));
            tokenizer.reset();
            CharTermAttribute charTermAttribute = tokenizer.getAttribute(CharTermAttribute.class);
            while (tokenizer.incrementToken()) {
                terms.add(charTermAttribute.toString());
            }
        }
        assertEquals(
            List.of(
                WildcardFieldTypeTests.prefixAnchored("a"),
                WildcardFieldTypeTests.suffixAnchored((char) 0 + "a"),
                WildcardFieldTypeTests.suffixAnchored("a")
            ),
            terms
        );
    }

    public void testEnableDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "wildcard").field("doc_values", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_SET, fields[1].fieldType().docValuesType());

        // doc_values is true by default
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "wildcard")));
        doc = mapper.parse(source(b -> b.field("field", "1234")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "wildcard").field("doc_values", false)));
        doc = mapper.parse(source(b -> b.field("field", "1234")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
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

    public void testNormalizer() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "wildcard").field("normalizer", "lowercase").field("doc_values", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "AbC")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertTrue(fields[0] instanceof Field);
        Field textField = (Field) fields[0];
        List<String> terms = new ArrayList<>();
        try (TokenStream tokenStream = textField.tokenStreamValue()) {
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
            while (tokenStream.incrementToken()) {
                terms.add(charTermAttribute.toString());
            }
        }
        assertEquals(
            List.of(
                WildcardFieldTypeTests.prefixAnchored("a"),
                WildcardFieldTypeTests.prefixAnchored("ab"),
                "abc",
                WildcardFieldTypeTests.suffixAnchored("bc"),
                WildcardFieldTypeTests.suffixAnchored("c")
            ),
            terms
        );
        IndexableFieldType fieldType = fields[0].fieldType();
        assertTrue(fieldType.omitNorms());
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertEquals(IndexOptions.DOCS, fieldType.indexOptions());
        assertFalse(fieldType.storeTermVectors());
        assertFalse(fieldType.storeTermVectorOffsets());
        assertFalse(fieldType.storeTermVectorPositions());
        assertFalse(fieldType.storeTermVectorPayloads());
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(new BytesRef("abc"), fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertEquals(IndexOptions.NONE, fieldType.indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "wildcard").field("null_value", "uri").field("doc_values", true)));
        doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        doc = mapper.parse(source(b -> b.nullField("field")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertTrue(fields[0] instanceof Field);
        Field textField = (Field) fields[0];
        List<String> terms = new ArrayList<>();
        try (TokenStream tokenStream = textField.tokenStreamValue()) {
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
            while (tokenStream.incrementToken()) {
                terms.add(charTermAttribute.toString());
            }
        }
        assertEquals(
            List.of(
                WildcardFieldTypeTests.prefixAnchored("u"),
                WildcardFieldTypeTests.prefixAnchored("ur"),
                "uri",
                WildcardFieldTypeTests.suffixAnchored("ri"),
                WildcardFieldTypeTests.suffixAnchored("i")
            ),
            terms
        );
        assertEquals(new BytesRef("uri"), fields[1].binaryValue());
        assertEquals(IndexOptions.NONE, fields[1].fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fields[1].fieldType().docValuesType());
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping.toString(), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);

        assertTrue(fields[0] instanceof Field);
        Field textField = (Field) fields[0];
        List<String> terms = new ArrayList<>();
        try (TokenStream tokenStream = textField.tokenStreamValue()) {
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
            while (tokenStream.incrementToken()) {
                terms.add(charTermAttribute.toString());
            }
        }
        assertEquals(
            List.of(
                WildcardFieldTypeTests.prefixAnchored("1"),
                WildcardFieldTypeTests.prefixAnchored("12"),
                "123",
                "234",
                WildcardFieldTypeTests.suffixAnchored("34"),
                WildcardFieldTypeTests.suffixAnchored("4")
            ),
            terms
        );
        IndexableFieldType fieldType = fields[0].fieldType();
        assertTrue(fieldType.omitNorms());
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertEquals(IndexOptions.DOCS, fieldType.indexOptions());
        assertFalse(fieldType.storeTermVectors());
        assertFalse(fieldType.storeTermVectorOffsets());
        assertFalse(fieldType.storeTermVectorPositions());
        assertFalse(fieldType.storeTermVectorPayloads());
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new WildcardFieldMapper.Builder("field").build(context).fieldType();
        assertEquals(Collections.singletonList("value"), fetchSourceValue(mapper, "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(mapper, 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(mapper, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(mapper, "value", "format"));
        assertEquals("Field [field] of type [wildcard] doesn't support formats.", e.getMessage());

        MappedFieldType ignoreAboveMapper = new WildcardFieldMapper.Builder("field").ignoreAbove(4).build(context).fieldType();
        assertEquals(Collections.emptyList(), fetchSourceValue(ignoreAboveMapper, "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(ignoreAboveMapper, 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(ignoreAboveMapper, true));

        MappedFieldType normalizerMapper = new WildcardFieldMapper.Builder("field", createIndexAnalyzers(null)).normalizer("lowercase")
            .build(context)
            .fieldType();
        assertEquals(Collections.singletonList("value"), fetchSourceValue(normalizerMapper, "VALUE"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(normalizerMapper, 42L));
        assertEquals(Collections.singletonList("value"), fetchSourceValue(normalizerMapper, "value"));

        MappedFieldType nullValueMapper = new WildcardFieldMapper.Builder("field").nullValue("NULL").build(context).fieldType();
        assertEquals(Collections.singletonList("NULL"), fetchSourceValue(nullValueMapper, null));
    }

    public void testPossibleToDeriveSource_WhenCopyToPresent() throws IOException {
        FieldMapper.CopyTo copyTo = new FieldMapper.CopyTo.Builder().add("copy_to_field").build();
        WildcardFieldMapper mapper = getMapper(copyTo, Integer.MAX_VALUE, "default", true);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenIgnoreAbovePresent() throws IOException {
        WildcardFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), 100, "default", true);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenNormalizerPresent() throws IOException {
        WildcardFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), 100, "lowercase", true);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testPossibleToDeriveSource_WhenDocValuesDisabled() throws IOException {
        WildcardFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), Integer.MAX_VALUE, "default", false);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testDerivedValueFetching_DocValues() throws IOException {
        try (Directory directory = newDirectory()) {
            WildcardFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty(), Integer.MAX_VALUE, "default", true);
            String value = "keyword_value";
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(createDocument(mapper, value));
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

    public void testRequiredTermsQueryMatchesLongWildcard() throws IOException {
        String value = longValueExceedingClauseLimit();
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value)));
        ParsedDocument upperCaseDoc = mapperService.documentMapper()
            .parse(source(b -> b.field(FIELD_NAME, value.toUpperCase(Locale.ROOT))));
        ParsedDocument suffixDoc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value + "suffix")));
        QueryShardContext context = createQueryShardContext(mapperService);
        when(context.sourcePath(FIELD_NAME)).thenReturn(Set.of(FIELD_NAME));
        MappedFieldType fieldType = mapperService.fieldType(FIELD_NAME);
        Query exactQuery = fieldType.wildcardQuery(value, null, false, context);
        Query prefixQuery = fieldType.wildcardQuery(value + "*", null, false, context);

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(doc.rootDoc());
            iw.addDocument(upperCaseDoc.rootDoc());
            iw.addDocument(suffixDoc.rootDoc());
        }, reader -> {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            assertEquals(1, indexSearcher.count(exactQuery));
            assertEquals(2, indexSearcher.count(prefixQuery));
        });
    }

    public void testRequiredTermsQueryMatchesLongWildcardCaseInsensitive() throws IOException {
        String value = longValueExceedingClauseLimit();
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value)));
        ParsedDocument upperCaseDoc = mapperService.documentMapper()
            .parse(source(b -> b.field(FIELD_NAME, value.toUpperCase(Locale.ROOT))));
        ParsedDocument suffixDoc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value + "suffix")));
        QueryShardContext context = createQueryShardContext(mapperService);
        when(context.sourcePath(FIELD_NAME)).thenReturn(Set.of(FIELD_NAME));
        MappedFieldType fieldType = mapperService.fieldType(FIELD_NAME);
        Query exactQuery = fieldType.wildcardQuery(value, null, true, context);
        Query prefixQuery = fieldType.wildcardQuery(value + "*", null, true, context);

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(doc.rootDoc());
            iw.addDocument(upperCaseDoc.rootDoc());
            iw.addDocument(suffixDoc.rootDoc());
        }, reader -> {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            assertEquals(2, indexSearcher.count(exactQuery));
            assertEquals(3, indexSearcher.count(prefixQuery));
        });
    }

    public void testRequiredTermsQueryMatchesLongRegexp() throws IOException {
        String value = longValueExceedingClauseLimit();
        Set<String> requiredTerms = WildcardFieldMapper.WildcardFieldType.getRequiredNGrams("*" + value + "*", true);
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value)));
        ParsedDocument upperCaseDoc = mapperService.documentMapper()
            .parse(source(b -> b.field(FIELD_NAME, value.toUpperCase(Locale.ROOT))));
        ParsedDocument suffixDoc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value + "suffix")));
        QueryShardContext context = createQueryShardContext(mapperService);
        when(context.sourcePath(FIELD_NAME)).thenReturn(Set.of(FIELD_NAME));
        Query query = mapperService.fieldType(FIELD_NAME).regexpQuery(value, 0, 0, 10_000, null, context);
        Query approximation = new WildcardFieldMapper.RequiredTermsQuery(FIELD_NAME, requiredTerms, false);

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(doc.rootDoc());
            iw.addDocument(upperCaseDoc.rootDoc());
            iw.addDocument(suffixDoc.rootDoc());
        }, reader -> {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            // The suffix document contains every required trigram but cannot match the full regexp.
            assertEquals(2, indexSearcher.count(approximation));
            assertEquals(1, indexSearcher.count(query));
        });
    }

    public void testRequiredTermsQueryMatchesLongRegexpCaseInsensitive() throws IOException {
        String value = longValueExceedingClauseLimit();
        Set<String> requiredTerms = WildcardFieldMapper.WildcardFieldType.getRequiredNGrams("*" + value + "*", true);
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value)));
        ParsedDocument upperCaseDoc = mapperService.documentMapper()
            .parse(source(b -> b.field(FIELD_NAME, value.toUpperCase(Locale.ROOT))));
        ParsedDocument suffixDoc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value + "suffix")));
        QueryShardContext context = createQueryShardContext(mapperService);
        when(context.sourcePath(FIELD_NAME)).thenReturn(Set.of(FIELD_NAME));
        Query query = mapperService.fieldType(FIELD_NAME).regexpQuery(value, 0, RegExp.ASCII_CASE_INSENSITIVE, 10_000, null, context);
        Query approximation = new WildcardFieldMapper.RequiredTermsQuery(FIELD_NAME, requiredTerms, true);

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(doc.rootDoc());
            iw.addDocument(upperCaseDoc.rootDoc());
            iw.addDocument(suffixDoc.rootDoc());
        }, reader -> {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            assertEquals(3, indexSearcher.count(approximation));
            assertEquals(2, indexSearcher.count(query));
        });
    }

    public void testRequiredTermsQueryPreservesShortPrefixes() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        String value = "abcdefghijklmnopqrstuvwxyz";
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value)));
        String pattern = "*a*b*c*d*e*f*g*h*i*j*k*l*m*n*o*p*";
        QueryShardContext context = createQueryShardContext(mapperService);
        when(context.sourcePath(FIELD_NAME)).thenReturn(Set.of(FIELD_NAME));
        Query query = mapperService.fieldType(FIELD_NAME).wildcardQuery(pattern, null, false, context);
        assertTrue(((WildcardFieldMapper.WildcardMatchingQuery) query).getSecondPhaseMatcher().test(value));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            // There is no exact term "a", but prefixes such as "a" match indexed trigrams such as "abc".
            assertEquals(0, reader.docFreq(new Term(FIELD_NAME, "a")));
            assertEquals(1, reader.docFreq(new Term(FIELD_NAME, "abc")));
            assertEquals(
                "A matching document must not be discarded when the required terms are short prefixes",
                1,
                new IndexSearcher(reader).count(query)
            );
        });
    }

    public void testRequiredTermsQueryRespectsConfiguredClauseLimit() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, "abcdefghijklmnopqrstuvwxyz")));
        QueryShardContext context = createQueryShardContext(mapperService);
        when(context.sourcePath(FIELD_NAME)).thenReturn(Set.of(FIELD_NAME));
        String pattern = "*abcdefghijkl*";
        Set<String> requiredTerms = WildcardFieldMapper.WildcardFieldType.getRequiredNGrams(pattern, false);
        assertEquals(10, requiredTerms.size());

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : List.of("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij")) {
            builder.add(new TermQuery(new Term(FIELD_NAME, term)), BooleanClause.Occur.FILTER);
        }

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            int originalMaxClauseCount = IndexSearcher.getMaxClauseCount();
            try {
                IndexSearcher.setMaxClauseCount(8);
                // Ten distinct trigrams must be reduced to eight clauses before building the BooleanQuery.
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                Query approximation = new WildcardFieldMapper.RequiredTermsQuery(FIELD_NAME, requiredTerms, false);
                Query query = mapperService.fieldType(FIELD_NAME).wildcardQuery(pattern, null, false, context);
                assertEquals(builder.build(), approximation.rewrite(indexSearcher));
                assertEquals(1, indexSearcher.count(query));
            } finally {
                IndexSearcher.setMaxClauseCount(originalMaxClauseCount);
            }
        });
    }

    public void testRequiredTermsQueryRejectsMissingTrigramDuringRewrite() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, "abcdefghijklmnopqrstuvwxyz")));
        Query query = new WildcardFieldMapper.RequiredTermsQuery(FIELD_NAME, Set.of("000"), true);

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(AutomatonQueries.caseInsensitiveTermQuery(new Term(FIELD_NAME, "000")), BooleanClause.Occur.FILTER);

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            assertEquals(0, reader.docFreq(new Term(FIELD_NAME, "000")));
            assertEquals(1, reader.docFreq(new Term(FIELD_NAME, "abc")));
            assertEquals(builder.build(), query.rewrite(indexSearcher));
            assertEquals(0, indexSearcher.count(query));
        });
    }

    public void testRequiredTermsQuerySelectsLowestFrequencies() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        Set<String> requiredTerms = new HashSet<>();
        List<String> values = new ArrayList<>();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (int i = 0; i < 20; i++) {
            String term = String.valueOf((char) ('a' + i)).repeat(3);
            requiredTerms.add(term);
            for (int document = 0; document <= i; document++) {
                values.add(term);
            }
            if (i < 16) {
                builder.add(new TermQuery(new Term(FIELD_NAME, term)), BooleanClause.Occur.FILTER);
            }
        }
        Query query = new WildcardFieldMapper.RequiredTermsQuery(FIELD_NAME, requiredTerms, false);

        withLuceneIndex(mapperService, iw -> {
            for (String value : values) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value)));
                iw.addDocument(doc.rootDoc());
            }
        }, reader -> {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            assertEquals(builder.build(), query.rewrite(indexSearcher));
        });
    }

    public void testRequiredTermsQuerySelectsLowestFrequenciesCaseInsensitive() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        Set<String> requiredTerms = new HashSet<>();
        List<String> values = new ArrayList<>();
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (int i = 0; i < 20; i++) {
            String term = String.valueOf((char) ('a' + i)).repeat(3);
            requiredTerms.add(term);
            for (int document = 0; document <= i; document++) {
                values.add(term.toUpperCase(Locale.ROOT));
            }
            if (i < 16) {
                builder.add(AutomatonQueries.caseInsensitiveTermQuery(new Term(FIELD_NAME, term)), BooleanClause.Occur.FILTER);
            }
        }
        Query query = new WildcardFieldMapper.RequiredTermsQuery(FIELD_NAME, requiredTerms, true);

        withLuceneIndex(mapperService, iw -> {
            for (String value : values) {
                ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(FIELD_NAME, value)));
                iw.addDocument(doc.rootDoc());
            }
        }, reader -> {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            assertEquals(0, reader.docFreq(new Term(FIELD_NAME, "aaa")));
            assertEquals(1, reader.docFreq(new Term(FIELD_NAME, "AAA")));
            assertEquals(builder.build(), query.rewrite(indexSearcher));
        });
    }

    private String longValueExceedingClauseLimit() {
        StringBuilder builder = new StringBuilder();
        for (char first = 'a'; first <= 'z'; first++) {
            for (char second = 'a'; second <= 'z'; second++) {
                builder.append(first).append(second).append('x');
            }
        }
        String value = builder.toString();
        Set<String> requiredTerms = WildcardFieldMapper.WildcardFieldType.getRequiredNGrams("*" + value + "*", false);
        assertTrue(value.length() > 2000);
        assertTrue(requiredTerms.size() > IndexSearcher.getMaxClauseCount());
        return value;
    }

    private WildcardFieldMapper getMapper(FieldMapper.CopyTo copyTo, int ignoreAbove, String normalizerName, boolean hasDocValues)
        throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "wildcard")
                    .field("doc_values", hasDocValues)
                    .field("normalizer", normalizerName)
                    .field("ignore_above", ignoreAbove)
            )
        );
        WildcardFieldMapper mapper = (WildcardFieldMapper) mapperService.documentMapper().mappers().getMapper(FIELD_NAME);
        mapper.copyTo = copyTo;
        return mapper;
    }

    /**
     * Helper method to create a document with both doc values and stored fields
     */
    private Document createDocument(WildcardFieldMapper mapper, String value) throws IOException {
        Document doc = new Document();
        NamedAnalyzer normalizer = mapper.fieldType().normalizer();
        value = normalizeValue(normalizer, FIELD_NAME, value);
        final BytesRef binaryValue = new BytesRef(value);
        doc.add(new SortedSetDocValuesField(FIELD_NAME, binaryValue));
        return doc;
    }

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
    }
}
