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
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class TermsEnumCollectorTests extends OpenSearchTestCase {

    private static final String FIELD = "tag";

    private final KeywordFieldType fieldType = new KeywordFieldType(FIELD);

    public void testTermOnlyInDeletedDocIsSkipped() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "ghost");
            addDoc(writer, "2", "live");
            writer.deleteDocuments(new Term("id", "1"));
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(1, reader.leaves().size());
                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> false);
                assertEquals(List.of("live"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    public void testTermInDeletedAndLiveDocIsKept() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "both");
            addDoc(writer, "2", "both");
            addDoc(writer, "3", "x");
            writer.deleteDocuments(new Term("id", "1"));
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(1, reader.leaves().size());
                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> false);
                assertEquals(List.of("both", "x"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    public void testDocsHiddenByReaderWrapperAreSkipped() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "ghost");
            addDoc(writer, "2", "live");
            try (DirectoryReader reader = new HidingDirectoryReader(DirectoryReader.open(writer), Set.of("1"))) {
                assertEquals(1, reader.leaves().size());
                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> false);
                assertEquals(List.of("live"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    public void testDocsHiddenWithoutLoweringNumDocsAreSkipped() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "ghost");
            addDoc(writer, "2", "live");
            try (DirectoryReader reader = new HidingDirectoryReader(DirectoryReader.open(writer), Set.of("1"), false)) {
                assertEquals(1, reader.leaves().size());
                // only the segment's live docs hide doc 1; the reader-level counts claim nothing is hidden
                assertNotNull(reader.leaves().get(0).reader().getLiveDocs());
                assertEquals(reader.maxDoc(), reader.numDocs());
                assertFalse(reader.hasDeletions());
                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> false);
                assertEquals(List.of("live"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    public void testTermInEveryDocHiddenWithoutLoweringNumDocsIsSkipped() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "1", "all");
            addDoc(writer, "2", "all");
            try (DirectoryReader reader = new HidingDirectoryReader(DirectoryReader.open(writer), Set.of("1", "2"), false)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(reader.maxDoc(), reader.numDocs());
                TermsEnum terms = MultiTerms.getTerms(reader, FIELD).iterator();
                assertTrue(terms.seekExact(new BytesRef("all")));
                assertEquals(reader.maxDoc(), terms.docFreq());

                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> false);
                assertEquals(List.of(), result.terms());
                assertFalse(result.timedOut());
            }
            try (DirectoryReader reader = new HidingDirectoryReader(DirectoryReader.open(writer), Set.of("1"), false)) {
                assertEquals(List.of("all"), collectAll(reader, null, 100, () -> false).terms());
            }
        }
    }

    public void testLiveDocsAreCheckedPerSegment() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            addDoc(writer, "0", "ghost");
            addDoc(writer, "1", "shared");
            addDoc(writer, "2", "keep");
            writer.commit();
            addDoc(writer, "3", "shared");
            addDoc(writer, "4", "late");
            writer.deleteDocuments(new Term("id", "0"), new Term("id", "1"));
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(2, reader.leaves().size());
                assertNotNull(reader.leaves().get(0).reader().getLiveDocs());
                assertNull(reader.leaves().get(1).reader().getLiveDocs());
                // "shared" is deleted in the first segment but live in the second, which has no deletions
                assertEquals(List.of("keep", "late", "shared"), collectAll(reader, null, 100, () -> false).terms());
            }
            try (DirectoryReader reader = new HidingDirectoryReader(DirectoryReader.open(writer), Set.of("3", "4"), false)) {
                assertEquals(2, reader.leaves().size());
                // the second segment now hides its documents too, so "shared" has no live document left
                assertEquals(List.of("keep"), collectAll(reader, null, 100, () -> false).terms());
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
                assertEquals(numberedTerms("t%03d", 128), result.terms());
                assertTrue(result.timedOut());
            }
        }
    }

    public void testSkippedCandidatesCountTowardDeadline() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            List<String> deleted = numberedTerms("d%03d", 200);
            for (int i = 0; i < deleted.size(); i++) {
                addDoc(writer, Integer.toString(i), deleted.get(i));
            }
            addDoc(writer, "200", "z-live");
            for (int i = 0; i < deleted.size(); i++) {
                writer.deleteDocuments(new Term("id", Integer.toString(i)));
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(List.of("z-live"), collectAll(reader, null, 100, () -> false).terms());

                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> true);
                assertEquals(List.of(), result.terms());
                assertTrue(result.timedOut());
            }
        }
    }

    public void testLongPostingsWalkChecksDeadline() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = newWriter(dir)) {
            for (int i = 0; i < 10000; i++) {
                addDoc(writer, Integer.toString(i), "common");
            }
            addDoc(writer, "10000", "z-live");
            writer.deleteDocuments(new Term(FIELD, "common"));
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(1, reader.leaves().size());
                TermsEnumCollector.Result withoutDeadline = collectAll(reader, null, 100, () -> false);
                assertEquals(List.of("z-live"), withoutDeadline.terms());
                assertFalse(withoutDeadline.timedOut());

                AtomicInteger calls = new AtomicInteger();
                TermsEnumCollector.Result result = collectAll(reader, null, 100, () -> {
                    calls.incrementAndGet();
                    return true;
                });
                assertEquals(List.of(), result.terms());
                assertTrue(result.timedOut());
                assertThat(calls.get(), greaterThanOrEqualTo(1));
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
                TermsEnum candidates = MultiTerms.getTerms(reader, FIELD).iterator();
                TermsEnumCollector.Result result = TermsEnumCollector.collect(
                    fieldType,
                    candidates,
                    reader,
                    new BytesRef("b"),
                    100,
                    () -> false
                );
                assertEquals(List.of("c"), result.terms());
                assertFalse(result.timedOut());
            }
        }
    }

    private TermsEnumCollector.Result collectAll(IndexReader reader, BytesRef searchAfter, int size, BooleanSupplier timedOut)
        throws IOException {
        TermsEnum candidates = fieldType.termsEnum(reader, "", false, searchAfter, mock(QueryShardContext.class));
        return TermsEnumCollector.collect(fieldType, candidates, reader, searchAfter, size, timedOut);
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
