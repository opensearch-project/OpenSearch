/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BooleanSupplier;

public class TermsEnumCollectorTests extends OpenSearchTestCase {

    private static final String FIELD = "tag";

    private final KeywordFieldType fieldType = new KeywordFieldType(FIELD);

    public void testTermOfDeletedDocIsReturnedUntilMerged() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "ghost");
            addDoc(writer, "2", "live");
            writer.deleteDocuments(new Term("id", "1"));
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(1, reader.leaves().size());
                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> false);
                assertEquals(List.of("ghost", "live"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    public void testTermsAreMergedAcrossSegments() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "b");
            addDoc(writer, "2", "d");
            writer.commit();
            addDoc(writer, "3", "a");
            addDoc(writer, "4", "b");
            addDoc(writer, "5", "c");
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(2, reader.leaves().size());
                assertEquals(List.of("a", "b", "c", "d"), collectAll(reader, null, 100, () -> false).terms());
                assertEquals(List.of("a", "b"), collectAll(reader, null, 2, () -> false).terms());
                assertEquals(List.of("c", "d"), collectAll(reader, new BytesRef("b"), 2, () -> false).terms());
            }
        }
    }

    public void testSizeStopsEnumeration() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            List<String> values = List.of("a", "b", "c", "d", "e");
            for (int i = 0; i < values.size(); i++) {
                addDoc(writer, Integer.toString(i), values.get(i));
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                TermsEnumCollector.Result result = collectAll(reader, null, 3, () -> false);
                assertEquals(List.of("a", "b", "c"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    public void testDeadlineReturnsTermsCollectedSoFar() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            indexNumberedTerms(writer, 300);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                TermsEnumCollector.Result result = collectAll(reader, null, 300, () -> true);
                assertEquals(numberedTerms("t%03d", TermsEnumCollector.CHECK_INTERVAL), result.terms());
                assertTrue(result.timedOut());
            }
        }
    }

    public void testCancellationPropagates() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            indexNumberedTerms(writer, 300);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                expectThrows(
                    TaskCancelledException.class,
                    () -> collectAll(reader, null, 300, () -> { throw new TaskCancelledException("cancelled"); })
                );
            }
        }
    }

    public void testSearchAfterIsStrict() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "a");
            addDoc(writer, "2", "b");
            addDoc(writer, "3", "c");
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                TermsEnumCollector.Result result = collectAll(reader, new BytesRef("b"), 100, () -> false);
                assertEquals(List.of("c"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    private TermsEnumCollector.Result collectAll(IndexReader reader, BytesRef searchAfter, int size, BooleanSupplier timedOut)
        throws IOException {
        return TermsEnumCollector.collect(fieldType, reader, "", false, searchAfter, size, timedOut);
    }

    private static IndexWriter newWriter(Directory dir) throws IOException {
        return new IndexWriter(
            dir,
            newIndexWriterConfig().setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH).setMergePolicy(NoMergePolicy.INSTANCE)
        );
    }

    private static void addDoc(IndexWriter writer, String id, String value) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.NO));
        doc.add(new StringField(FIELD, value, Field.Store.NO));
        writer.addDocument(doc);
    }

    private static void indexNumberedTerms(IndexWriter writer, int count) throws IOException {
        List<String> values = numberedTerms("t%03d", count);
        for (int i = 0; i < values.size(); i++) {
            addDoc(writer, Integer.toString(i), values.get(i));
        }
    }

    private static List<String> numberedTerms(String format, int count) {
        List<String> terms = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            terms.add(String.format(Locale.ROOT, format, i));
        }
        return terms;
    }
}
