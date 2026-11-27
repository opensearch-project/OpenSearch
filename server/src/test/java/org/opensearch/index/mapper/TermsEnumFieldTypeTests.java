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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.termsenum.TermsEnumCollector;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.LowercaseNormalizer;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    public void testKeywordPrefix() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(
            KEYWORD_VALUES,
            reader -> { assertEquals(List.of("web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", false, null)); }
        );
    }

    public void testKeywordEmptyPrefixReturnsAllInByteOrder() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, reader -> {
            assertEquals(
                List.of("WEB-99", "Web-03", "a*b", "a.b", "apple", "web-01", "web-02", "web-10", "wec", "Émile", "émile"),
                enumerate(ft, reader, "", false, null)
            );
        });
    }

    public void testKeywordCaseInsensitive() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, reader -> {
            assertEquals(List.of("WEB-99", "Web-03", "web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", true, null));
        });
    }

    public void testKeywordCaseInsensitiveIsAsciiOnly() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, reader -> { assertEquals(List.of("émile"), enumerate(ft, reader, "émile", true, null)); });
    }

    public void testKeywordPrefixMetacharactersAreLiteral() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, reader -> {
            for (boolean caseInsensitive : new boolean[] { false, true }) {
                assertEquals(List.of("a*b"), enumerate(ft, reader, "a*", caseInsensitive, null));
                assertEquals(List.of("a.b"), enumerate(ft, reader, "a.", caseInsensitive, null));
                assertEquals(List.of(), enumerate(ft, reader, "a?", caseInsensitive, null));
            }
        });
    }

    public void testKeywordSearchAfter() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, reader -> {
            assertEquals(List.of("web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", false, "a"));
            assertEquals(List.of("web-02", "web-10"), enumerate(ft, reader, "web-", false, "web-01"));
            assertEquals(List.of("web-10"), enumerate(ft, reader, "web-", false, "web-05"));
            assertEquals(List.of(), enumerate(ft, reader, "web-", false, "web-10"));
            assertEquals(List.of(), enumerate(ft, reader, "web-", false, "zzz"));
        });
    }

    public void testKeywordCaseInsensitiveSearchAfter() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, reader -> {
            assertEquals(List.of("web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", true, "Web-03"));
            // "Wa" is not accepted by the case-insensitive automaton and sorts between "WEB-99" and "Web-03"
            assertEquals(List.of("Web-03", "web-01", "web-02", "web-10"), enumerate(ft, reader, "web-", true, "Wa"));
        });
    }

    public void testKeywordCaseInsensitiveEmptyPrefix() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        withReader(KEYWORD_VALUES, reader -> {
            assertEquals(
                List.of("WEB-99", "Web-03", "a*b", "a.b", "apple", "web-01", "web-02", "web-10", "wec", "Émile", "émile"),
                enumerate(ft, reader, "", true, null)
            );
            assertEquals(List.of("web-01", "web-02", "web-10", "wec", "Émile", "émile"), enumerate(ft, reader, "", true, "apple"));
        });
    }

    public void testKeywordNormalizerAppliesToPrefix() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()));
        withReader(
            List.of("web-01", "web-02"),
            reader -> { assertEquals(List.of("web-01", "web-02"), enumerate(ft, reader, "WEB-", false, null)); }
        );
    }

    public void testKeywordNotIndexedIsRejected() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field", false, true, Map.of());
        withReader(KEYWORD_VALUES, reader -> {
            LeafReader leaf = reader.leaves().get(0).reader();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.termsEnum(leaf, "", false, null));
            assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
        });
    }

    public void testKeywordFieldMissingFromReaderReturnsNull() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("missing");
        withReader(KEYWORD_VALUES, reader -> {
            assertNull(ft.termsEnum(reader.leaves().get(0).reader(), "", false, null));
            assertEquals(List.of(), enumerate(ft, reader, "", false, null));
        });
    }

    private List<String> enumerate(TermBasedFieldType ft, IndexReader r, String prefix, boolean ci, String searchAfter) throws IOException {
        BytesRef after = searchAfter == null ? null : new BytesRef(searchAfter);
        return TermsEnumCollector.collect(ft, r, prefix, ci, after, 100, () -> false).terms();
    }

    /** Indexes one document per value into the keyword field {@code field} and runs {@code test} on a reader over them. */
    private void withReader(List<String> values, CheckedConsumer<DirectoryReader, IOException> test) throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            for (int i = 0; i < values.size(); i++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                doc.add(new StringField("field", values.get(i), Field.Store.NO));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
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
