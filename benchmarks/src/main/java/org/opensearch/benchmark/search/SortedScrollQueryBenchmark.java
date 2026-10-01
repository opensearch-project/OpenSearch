/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.opensearch.lucene.queries.SearchAfterSortedDocQuery;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Measures the query phase of later index-sorted scroll pages, excluding stored-field fetch. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SortedScrollQueryBenchmark {
    @Param({ "50", "0" })
    public int filterPercent;

    @Param({ "100" })
    public int pageSize;

    @Param({ "1" })
    public int segments;

    @Param({ "false" })
    public boolean reverse;

    @Param({ "false" })
    public boolean deletions;

    @Param({ "false" })
    public boolean firstPage;

    private Path temporaryIndex;
    private FSDirectory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Sort sort;
    private Query filter;
    private FieldDoc[] positions;
    private int cursor;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        String indexPath = System.getProperty("benchmark.index");
        if (indexPath == null) {
            temporaryIndex = Files.createTempDirectory("sorted-scroll-benchmark");
            createIndex(temporaryIndex, segments, reverse, deletions);
            indexPath = temporaryIndex.toString();
        }
        directory = FSDirectory.open(Path.of(indexPath));
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        sort = new Sort(new SortField("sort", SortField.Type.LONG, reverse));
        filter = filterPercent == 0 ? new MatchAllDocsQuery() : new TermQuery(new Term("filter" + filterPercent, "match"));
        positions = new FieldDoc[32];
        for (int i = 0; i < positions.length; i++) {
            int doc = i * (reader.maxDoc() / (2 * positions.length));
            LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(doc, reader.leaves()));
            NumericDocValues values = leaf.reader().getNumericDocValues("sort");
            if (values.advanceExact(doc - leaf.docBase) == false) {
                throw new IllegalStateException("Missing sort value");
            }
            positions[i] = new FieldDoc(doc, Float.NaN, new Object[] { values.longValue() });
            TopDocs expected = searcher.searchAfter(firstPage ? null : positions[i], filter, pageSize, sort);
            TopFieldDocs actual = searcher.search(query(positions[i]), new TopFieldCollectorManager(sort, pageSize, null, 0));
            if (expected.scoreDocs.length != actual.scoreDocs.length || actual.scoreDocs.length == 0) {
                throw new IllegalStateException("Unexpected page length");
            }
            for (int j = 0; j < expected.scoreDocs.length; j++) {
                if (expected.scoreDocs[j].doc != actual.scoreDocs[j].doc
                    || Arrays.equals(((FieldDoc) expected.scoreDocs[j]).fields, ((FieldDoc) actual.scoreDocs[j]).fields) == false) {
                    throw new IllegalStateException("Pagination differs from searchAfter");
                }
            }
        }
    }

    private Query query(FieldDoc after) {
        if (firstPage) {
            return filter;
        }
        return new BooleanQuery.Builder().add(filter, BooleanClause.Occur.MUST)
            .add(new SearchAfterSortedDocQuery(sort, after), BooleanClause.Occur.FILTER)
            .build();
    }

    @Benchmark
    public TopFieldDocs search() throws IOException {
        return searcher.search(query(positions[cursor++ & 31]), new TopFieldCollectorManager(sort, pageSize, null, 0));
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        IOUtils.close(reader, directory);
        if (temporaryIndex != null) {
            IOUtils.rm(temporaryIndex);
        }
    }

    /** Creates an immutable fixture that can also be shared between separate baseline and candidate JVMs. */
    public static void createIndex(Path path, int segments, boolean reverse, boolean deletions) throws IOException {
        final int documents = 1 << 20;
        Sort sort = new Sort(new SortField("sort", SortField.Type.LONG, reverse));
        IndexWriterConfig config = new IndexWriterConfig().setIndexSort(sort)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setRAMBufferSizeMB(256);
        try (FSDirectory directory = FSDirectory.open(path); IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < documents; i++) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("sort", i / 3));
                if (i % 100 == 0) {
                    doc.add(new StringField("filter1", "match", Field.Store.NO));
                }
                if (i % 2 == 0) {
                    doc.add(new StringField("filter50", "match", Field.Store.NO));
                }
                if (i % 10 != 0) {
                    doc.add(new StringField("filter90", "match", Field.Store.NO));
                }
                if (deletions) {
                    doc.add(new IntPoint("delete", i % 10));
                }
                writer.addDocument(doc);
                if ((i + 1) % (documents / segments) == 0) {
                    writer.commit();
                }
            }
            if (deletions) {
                writer.deleteDocuments(IntPoint.newExactQuery("delete", 0));
            }
            writer.commit();
        }
    }
}
