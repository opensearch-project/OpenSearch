/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.opensearch.action.termsenum.HidingDirectoryReader;
import org.opensearch.action.termsenum.TermsEnumCollector;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.core.index.Index;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.LowercaseNormalizer;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsEnumFieldTypeTests extends OpenSearchTestCase {

    private static final List<String> KEYWORD_VALUES = List.of(
        "web-01",
        "web-02",
        "web-10",
        "Web-03",
        "WEB-99",
        "wec",
        "apple",
        "a*b",
        "a.b",
        "émile",
        "Émile"
    );

    private final QueryShardContext ctx = mock(QueryShardContext.class);

    // keyword

    public void testKeywordPrefix() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            assertEquals(List.of("web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", false, null));
        });
    }

    public void testKeywordEmptyPrefixReturnsAllInByteOrder() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            assertEquals(
                List.of("WEB-99", "Web-03", "a*b", "a.b", "apple", "web-01", "web-02", "web-10", "wec", "Émile", "émile"),
                enumerate(ft, reader, "", false, null)
            );
        });
    }

    public void testKeywordCaseInsensitive() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            assertEquals(List.of("WEB-99", "Web-03", "web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", true, null));
        });
    }

    public void testKeywordCaseInsensitiveIsAsciiOnly() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> { assertEquals(List.of("émile"), enumerate(ft, reader, "émile", true, null)); });
    }

    public void testKeywordPrefixMetacharactersAreLiteral() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            for (boolean caseInsensitive : new boolean[] { false, true }) {
                assertEquals(List.of("a*b"), enumerate(ft, reader, "a*", caseInsensitive, null));
                assertEquals(List.of("a.b"), enumerate(ft, reader, "a.", caseInsensitive, null));
                assertEquals(List.of(), enumerate(ft, reader, "a?", caseInsensitive, null));
            }
        });
    }

    public void testKeywordSearchAfter() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            assertEquals(List.of("web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", false, "a"));
            assertEquals(List.of("web-02", "web-10"), enumerate(ft, reader, "web-", false, "web-01"));
            assertEquals(List.of("web-10"), enumerate(ft, reader, "web-", false, "web-05"));
            assertEquals(List.of(), enumerate(ft, reader, "web-", false, "web-10"));
            assertEquals(List.of(), enumerate(ft, reader, "web-", false, "zzz"));
        });
    }

    public void testKeywordCaseInsensitiveSearchAfter() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            assertEquals(List.of("web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", true, "Web-03"));
            // "Wa" is not accepted by the case-insensitive automaton and sorts between "WEB-99" and "Web-03"
            assertEquals(List.of("Web-03", "web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", true, "Wa"));
        });
    }

    public void testKeywordCaseInsensitiveEmptyPrefix() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            // an empty prefix compiles to an automaton that accepts everything, which Terms#intersect rejects
            assertEquals(
                List.of("WEB-99", "Web-03", "a*b", "a.b", "apple", "web-01", "web-02", "web-10", "wec", "Émile", "émile"),
                enumerate(ft, reader, "", true, null)
            );
            assertEquals(List.of("web-01", "web-02", "web-10", "wec", "Émile", "émile"), enumerate(ft, reader, "", true, "apple"));
        });
    }

    public void testFilterFieldTypeDelegatesTermsEnum() throws IOException {
        KeywordFieldType delegate = new KeywordFieldType("field");
        MappedFieldType filtered = new FilterFieldType(delegate) {
            @Override
            public String typeName() {
                return "filtered";
            }
        };
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            assertEquals(List.of("WEB-99", "Web-03", "web-01", "web-02", "web-10"), enumerate(filtered, reader, "web-", true, null));
            assertEquals(List.of("web-02", "web-10"), enumerate(filtered, reader, "web-", false, "web-01"));
        });
    }

    public void testKeywordNormalizerAppliesToPrefix() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()));
        withReader(List.of("web-01", "web-02"), Set.of(), reader -> {
            assertEquals(List.of("web-01", "web-02"), enumerate(ft, reader, "WEB-", false, null));
        });
    }

    public void testKeywordNotIndexedIsRejected() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field", false, true, Map.of());
        try (MultiReader reader = new MultiReader()) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.termsEnum(reader, "", false, null, ctx));
            assertEquals("Field [field] of type [keyword] does not support terms enumeration because it is not indexed", e.getMessage());
        }
    }

    public void testKeywordFieldMissingFromReaderReturnsNull() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("missing");
        withReader(KEYWORD_VALUES, Set.of(), reader -> {
            assertNull(ft.termsEnum(reader, "", false, null, ctx));
            assertEquals(List.of(), enumerate(ft, reader, "", false, null));
        });
    }

    public void testDefaultHookThrows() throws IOException {
        try (MultiReader reader = new MultiReader()) {
            MappedFieldType longType = new NumberFieldMapper.NumberFieldType("n", NumberType.LONG);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> longType.termsEnum(reader, "", false, null, ctx)
            );
            assertEquals("Field [n] of type [long] does not support terms enumeration", e.getMessage());

            MappedFieldType textType = new TextFieldMapper.TextFieldType("t");
            e = expectThrows(IllegalArgumentException.class, () -> textType.termsEnum(reader, "", false, null, ctx));
            assertEquals("Field [t] of type [text] does not support terms enumeration", e.getMessage());
        }
    }

    // constant_keyword and _index

    public void testConstantPrefixMatch() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("team", "web-prod");
        withReader(List.of("a", "b"), Set.of(), reader -> {
            assertEquals(List.of("web-prod"), enumerate(ft, reader, "web", false, null));
            assertEquals(List.of("web-prod"), enumerate(ft, reader, "", false, null));
            assertEquals(List.of(), enumerate(ft, reader, "api", false, null));
        });
    }

    public void testConstantCaseInsensitiveIsAsciiOnly() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("team", "web-prod");
        MappedFieldType accented = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("name", "émile");
        withReader(List.of("a", "b"), Set.of(), reader -> {
            assertEquals(List.of("web-prod"), enumerate(ft, reader, "WEB", true, null));
            assertEquals(List.of(), enumerate(ft, reader, "WEB", false, null));
            assertEquals(List.of(), enumerate(accented, reader, "É", true, null));
        });
    }

    public void testConstantCaseInsensitiveLongPrefix() throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1500; i++) {
            // distinct CJK code points, with a lowercase ASCII letter every third character
            builder.appendCodePoint(i % 3 == 0 ? 'a' + i % 26 : 0x4E00 + i);
        }
        builder.appendCodePoint(0x1F600);
        String value = builder.toString();
        String upper = value.toUpperCase(Locale.ROOT);
        assertNotEquals(value, upper);
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("name", value);
        withReader(List.of("a"), Set.of(), reader -> {
            assertEquals(List.of(value), enumerate(ft, reader, upper, true, null));
            assertEquals(List.of(value), enumerate(ft, reader, value, true, null));
            assertEquals(List.of(value), enumerate(ft, reader, upper.substring(0, 1000), true, null));
            assertEquals(List.of(), enumerate(ft, reader, upper, false, null));
            // differs only in the last CJK character before the surrogate pair
            assertEquals(List.of(), enumerate(ft, reader, upper.substring(0, 1499) + "\u4E00", true, null));
            assertEquals(List.of(), enumerate(ft, reader, upper + "A", true, null));
        });
    }

    public void testConstantCaseInsensitiveMatchesCaseInsensitivePrefixAutomaton() throws IOException {
        // ASCII letters and their neighbours, the code points around the folding limit of 128, non-ASCII letters (the
        // Kelvin sign and the long s change case to ASCII letters), a surrogate pair and both lone surrogates
        List<String> alphabet = new ArrayList<>();
        "aAzZkKsS@[`{0\u007F\u0080\u0081éÉ\u212A\u017F".chars().forEach(c -> alphabet.add(String.valueOf((char) c)));
        alphabet.addAll(List.of("\uD83D\uDE00", "\uD83D", "\uDE00"));
        withReader(List.of("a"), Set.of(), reader -> {
            for (int i = 0; i < 500; i++) {
                String value = randomString(alphabet, between(0, 6));
                String prefix;
                if (randomBoolean() && value.isEmpty() == false) {
                    // a prefix of the value with the case of random characters flipped, so that matches are frequent
                    StringBuilder flipped = new StringBuilder();
                    value.substring(0, between(0, value.length())).codePoints().forEach(c -> {
                        if (randomBoolean()) {
                            c = Character.isUpperCase(c) ? Character.toLowerCase(c) : Character.toUpperCase(c);
                        }
                        flipped.appendCodePoint(c);
                    });
                    prefix = flipped.toString();
                } else {
                    prefix = randomString(alphabet, between(0, 3));
                }
                boolean expected = new CharacterRunAutomaton(AutomatonQueries.caseInsensitivePrefix(prefix)).run(value);
                MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("f", value);
                String message = "value [" + escape(value) + "] prefix [" + escape(prefix) + "]";
                assertEquals(message, expected, ft.termsEnum(reader, prefix, true, null, ctx) != null);
            }
        });
    }

    private static String randomString(List<String> alphabet, int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append(randomFrom(alphabet));
        }
        return builder.toString();
    }

    private static String escape(String s) {
        StringBuilder builder = new StringBuilder();
        s.chars().forEach(c -> builder.append(String.format(Locale.ROOT, "\\u%04X", c)));
        return builder.toString();
    }

    public void testConstantWildcardCharIsLiteral() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("team", "web-prod");
        withReader(List.of("a", "b"), Set.of(), reader -> {
            for (boolean caseInsensitive : new boolean[] { false, true }) {
                assertEquals(List.of(), enumerate(ft, reader, "w*", caseInsensitive, null));
                assertEquals(List.of(), enumerate(ft, reader, "web?", caseInsensitive, null));
            }
        });
    }

    public void testConstantSearchAfter() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("team", "web-prod");
        withReader(List.of("a", "b"), Set.of(), reader -> {
            assertEquals(List.of(), enumerate(ft, reader, "web", false, "web-prod"));
            assertEquals(List.of("web-prod"), enumerate(ft, reader, "web", false, "web"));
            assertEquals(List.of(), enumerate(ft, reader, "web", false, "zzz"));
        });
    }

    public void testConstantEmptyIndexReturnsNull() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("team", "web-prod");
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertNull(ft.termsEnum(reader, "", false, null, ctx));
            }
        }
    }

    public void testConstantNoLiveDocsReturnsNull() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("team", "web-prod");
        withReader(List.of("a", "b"), Set.of(), Set.of("0", "1"), reader -> {
            assertEquals(1, reader.leaves().size());
            assertEquals(0, reader.numDocs());
            assertNull(ft.termsEnum(reader, "", false, null, ctx));
        });
    }

    public void testConstantTermHiddenWithoutLoweringNumDocsIsSkipped() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("team", "web-prod");
        withReader(List.of("a", "b"), Set.of(), Set.of("0", "1"), false, reader -> {
            assertEquals(1, reader.leaves().size());
            // numDocs() still counts the hidden documents, so the hook synthesizes the term and the collector must drop it
            assertEquals(2, reader.numDocs());
            assertNotNull(ft.termsEnum(reader, "", false, null, ctx));
            assertEquals(List.of(), enumerate(ft, reader, "", false, null));
        });
        withReader(List.of("a", "b"), Set.of(), Set.of("0"), false, reader -> {
            assertEquals(List.of("web-prod"), enumerate(ft, reader, "", false, null));
        });
    }

    public void testIndexFieldUsesIndexName() throws IOException {
        when(ctx.index()).thenReturn(new Index("logs-2026", "_na_"));
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;
        withReader(List.of("a"), Set.of(), reader -> {
            assertEquals(1, reader.numDocs());
            assertEquals(List.of("logs-2026"), enumerate(ft, reader, "logs", false, null));
            assertEquals(List.of("logs-2026"), enumerate(ft, reader, "LOGS", true, null));
            assertEquals(List.of(), enumerate(ft, reader, "metrics", false, null));
        });
    }

    public void testConstantTermOnReaderWithDeletions() throws IOException {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("color", "red");
        withReader(List.of("a", "b"), Set.of("0"), reader -> {
            assertEquals(1, reader.leaves().size());
            assertEquals(1, reader.numDocs());
            assertEquals(List.of("red"), enumerate(ft, reader, "", false, null));
        });
    }

    public void testDisplayStringForConstantField() {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("color", "red");
        assertEquals("red", TermsEnumCollector.displayString(ft, new BytesRef("red")));
    }

    private List<String> enumerate(MappedFieldType ft, IndexReader r, String prefix, boolean ci, String searchAfter) throws IOException {
        BytesRef after = bytesOrNull(searchAfter);
        return TermsEnumCollector.collect(ft, ft.termsEnum(r, prefix, ci, after, ctx), r, after, 100, () -> false).terms();
    }

    private static BytesRef bytesOrNull(String value) {
        return value == null ? null : new BytesRef(value);
    }

    private void withReader(List<String> values, Set<String> deletedIds, CheckedConsumer<DirectoryReader, IOException> test)
        throws IOException {
        withReader(values, deletedIds, Set.of(), test);
    }

    private void withReader(
        List<String> values,
        Set<String> deletedIds,
        Set<String> hiddenIds,
        CheckedConsumer<DirectoryReader, IOException> test
    ) throws IOException {
        withReader(values, deletedIds, hiddenIds, true, test);
    }

    /**
     * Indexes one document per value into the keyword field {@code field}, with ids {@code 0..n-1}, deletes the
     * documents whose id is in {@code deletedIds}, and runs {@code test} on a reader opened after every write that
     * also hides the documents whose id is in {@code hiddenIds}. When {@code lowerNumDocs} is false, the reader
     * hides them through the live docs only and its {@code numDocs()} still counts them.
     */
    private void withReader(
        List<String> values,
        Set<String> deletedIds,
        Set<String> hiddenIds,
        boolean lowerNumDocs,
        CheckedConsumer<DirectoryReader, IOException> test
    ) throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            for (int i = 0; i < values.size(); i++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                doc.add(new StringField("field", values.get(i), Field.Store.NO));
                writer.addDocument(doc);
            }
            for (String id : deletedIds) {
                writer.deleteDocuments(new Term("id", id));
            }
            DirectoryReader opened = DirectoryReader.open(writer);
            try (DirectoryReader reader = hiddenIds.isEmpty() ? opened : new HidingDirectoryReader(opened, hiddenIds, lowerNumDocs)) {
                test.accept(reader);
            }
        }
    }

    private static IndexWriter newWriter(Directory dir) throws IOException {
        return new IndexWriter(
            dir,
            newIndexWriterConfig().setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH).setMergePolicy(NoMergePolicy.INSTANCE)
        );
    }
}
