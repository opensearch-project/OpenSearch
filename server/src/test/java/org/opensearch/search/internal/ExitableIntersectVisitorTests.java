/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IntsRef;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExitableIntersectVisitorTests extends OpenSearchTestCase {

    private static final int CHECK_INTERVAL = 1 << 13;
    private static final byte[] VALUE = new byte[Long.BYTES];

    private static class Cancellation implements ExitableDirectoryReader.QueryCancellation {
        int checks;
        boolean cancelled;

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void checkCancelled() {
            checks++;
            if (cancelled) {
                throw new TaskCancelledException("cancelled");
            }
        }
    }

    /** Records the visit methods called and the doc IDs accepted. */
    private static class RecordingVisitor implements PointValues.IntersectVisitor {
        final PointValues.Relation relation;
        final List<String> calls = new ArrayList<>();
        final List<Integer> docs = new ArrayList<>();

        RecordingVisitor(PointValues.Relation relation) {
            this.relation = relation;
        }

        private void addAll(DocIdSetIterator iterator) throws IOException {
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                docs.add(doc);
            }
        }

        @Override
        public void visit(int docID) {
            calls.add("doc");
            docs.add(docID);
        }

        @Override
        public void visit(DocIdSetIterator iterator) throws IOException {
            calls.add("iterator");
            addAll(iterator);
        }

        @Override
        public void visit(IntsRef ref) {
            calls.add("ints");
            for (int i = ref.offset; i < ref.offset + ref.length; i++) {
                docs.add(ref.ints[i]);
            }
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            calls.add("doc+value");
            docs.add(docID);
        }

        @Override
        public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            calls.add("iterator+value");
            addAll(iterator);
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return relation;
        }
    }

    private static ExitableDirectoryReader.ExitableIntersectVisitor wrap(PointValues.IntersectVisitor in, Cancellation cancellation) {
        ExitableDirectoryReader.ExitableIntersectVisitor visitor = new ExitableDirectoryReader.ExitableIntersectVisitor(cancellation);
        visitor.setVisitor(in);
        return visitor;
    }

    /** Calls {@code visitDocValues} on every leaf, which is where the exitable visitor is installed. */
    private static void visitLeaves(PointValues.PointTree tree, PointValues.IntersectVisitor visitor) throws IOException {
        if (tree.moveToChild()) {
            do {
                visitLeaves(tree, visitor);
            } while (tree.moveToSibling());
            tree.moveToParent();
        } else {
            tree.visitDocValues(visitor);
        }
    }

    private static RecordingVisitor visitLeaves(DirectoryReader reader, String field, PointValues.Relation relation) throws IOException {
        RecordingVisitor visitor = new RecordingVisitor(relation);
        visitLeaves(reader.leaves().get(0).reader().getPointValues(field).getPointTree(), visitor);
        return visitor;
    }

    public void testBulkVisitsArePreserved() throws IOException {
        try (Directory dir = newDirectory()) {
            // Production codec: randomized test codecs may not emit bulk visits.
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
                int cardinality = randomIntBetween(2, 8);
                for (int i = 0; i < 20_000; i++) {
                    Document doc = new Document();
                    // Low-cardinality 1D leaves are visited with visit(DocIdSetIterator, byte[]).
                    doc.add(new LongPoint("1d", i % cardinality));
                    // Multi-dimensional leaves inside the query are visited with visit(IntsRef).
                    doc.add(new LongPoint("2d", i, i / 2));
                    w.addDocument(doc);
                }
                w.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                DirectoryReader exitable = new ExitableDirectoryReader(reader, new Cancellation());
                assertBulkVisitsPreserved(reader, exitable, "1d", PointValues.Relation.CELL_CROSSES_QUERY, "iterator+value");
                assertBulkVisitsPreserved(reader, exitable, "2d", PointValues.Relation.CELL_INSIDE_QUERY, "ints");
            }
        }
    }

    private static void assertBulkVisitsPreserved(
        DirectoryReader reader,
        DirectoryReader exitable,
        String field,
        PointValues.Relation relation,
        String bulkCall
    ) throws IOException {
        RecordingVisitor expected = visitLeaves(reader, field, relation);
        RecordingVisitor actual = visitLeaves(exitable, field, relation);
        assertEquals(expected.calls, actual.calls);
        assertEquals(expected.docs, actual.docs);
        assertTrue(actual.calls.contains(bulkCall));
    }

    public void testBulkVisitsAreChargedToSamplingBudget() throws IOException {
        Cancellation cancellation = new Cancellation();
        RecordingVisitor delegate = new RecordingVisitor(PointValues.Relation.CELL_CROSSES_QUERY);
        ExitableDirectoryReader.ExitableIntersectVisitor visitor = wrap(delegate, cancellation);

        visitor.visit(0); // first visit checks
        assertEquals(1, cancellation.checks);
        visitor.visit(DocIdSetIterator.range(0, CHECK_INTERVAL - 3));
        visitor.visit(new IntsRef(new int[] { 1 }, 0, 1));
        visitor.visit(DocIdSetIterator.range(0, 1), VALUE); // uses the last slot of the budget
        assertEquals(1, cancellation.checks);
        visitor.visit(1, VALUE); // starts a new budget
        assertEquals(2, cancellation.checks);
        visitor.visit(DocIdSetIterator.range(0, CHECK_INTERVAL * 3)); // overshoots the budget
        assertEquals(2, cancellation.checks);
        cancellation.cancelled = true;
        expectThrows(TaskCancelledException.class, () -> visitor.visit(new IntsRef(new int[] { 1 }, 0, 1)));

        assertEquals(List.of("doc", "iterator", "ints", "iterator+value", "doc+value", "iterator"), delegate.calls);
    }

    public void testCancelledBulkVisitsThrowBeforeVisiting() {
        Cancellation cancellation = new Cancellation();
        cancellation.cancelled = true;
        RecordingVisitor delegate = new RecordingVisitor(PointValues.Relation.CELL_CROSSES_QUERY);
        expectThrows(TaskCancelledException.class, () -> wrap(delegate, cancellation).visit(DocIdSetIterator.range(0, 10)));
        expectThrows(TaskCancelledException.class, () -> wrap(delegate, cancellation).visit(new IntsRef(new int[] { 1 }, 0, 1)));
        expectThrows(TaskCancelledException.class, () -> wrap(delegate, cancellation).visit(DocIdSetIterator.range(0, 10), VALUE));
        assertTrue(delegate.calls.isEmpty());
    }
}
