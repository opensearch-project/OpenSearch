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
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.opensearch.index.mapper.FieldTypeTestCase.fetchSourceValue;

public class WildcardFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "wildcard");
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("value");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("normalizer", b -> b.field("normalizer", "lowercase"));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", true));
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
                "p",
                "pi",
                "pic",
                "i",
                "ic",
                "ick",
                "c",
                "ck",
                "ckl",
                "k",
                "kl",
                "kle",
                "l",
                "le",
                WildcardFieldTypeTests.suffixAnchored("le"),
                "e",
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
        assertEquals(List.of(WildcardFieldTypeTests.prefixAnchored("a"), "a", WildcardFieldTypeTests.suffixAnchored("a")), terms);
    }

    public void testEnableDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "wildcard").field("doc_values", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_SET, fields[1].fieldType().docValuesType());

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "wildcard")));
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
                "a",
                "ab",
                "abc",
                "b",
                "bc",
                WildcardFieldTypeTests.suffixAnchored("bc"),
                "c",
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
                "u",
                "ur",
                "uri",
                "r",
                "ri",
                WildcardFieldTypeTests.suffixAnchored("ri"),
                "i",
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
                "1",
                "12",
                "123",
                "2",
                "23",
                "234",
                "3",
                "34",
                WildcardFieldTypeTests.suffixAnchored("34"),
                "4",
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
}
