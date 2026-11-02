/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.DerivedFieldSupportedTypes;
import org.opensearch.index.mapper.DerivedFieldValueFetcher;
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.script.Script;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntPredicate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DerivedFieldQueryTests extends OpenSearchTestCase {

    private static final String[][] raw_requests = new String[][] {
        { "40.135.0.0 GET /images/hm_bg.jpg HTTP/1.0", "200", "40.135.0.0" },
        { "232.0.0.0 GET /images/hm_bg.jpg HTTP/1.0", "400", "232.0.0.0" },
        { "26.1.0.0 GET /images/hm_bg.jpg HTTP/1.0", "200", "26.1.0.0" },
        { "247.37.0.0 GET /french/splash_inet.html HTTP/1.0", "400", "247.37.0.0" },
        { "247.37.0.0 GET /french/splash_inet.html HTTP/1.0", "400", "247.37.0.0" },
        { "247.37.0.0 GET /french/splash_inet.html HTTP/1.0", "200", "247.37.0.0" } };

    public void testDerivedField() throws IOException {
        // Create lucene documents
        List<Document> docs = new ArrayList<>();
        for (String[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_request", request[0], Field.Store.YES));
            document.add(new KeywordField("status", request[1], Field.Store.YES));
            docs.add(document);
        }

        // Mock SearchLookup
        SearchLookup searchLookup = mock(SearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(leafLookup.source()).thenReturn(sourceLookup);

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> {
            when(searchLookup.getLeafSearchLookup(ctx)).thenReturn(leafLookup);
            return new DerivedFieldScript(params, lookup, ctx) {
                @Override
                public void execute() {
                    addEmittedValue(raw_requests[sourceLookup.docId()][2]);
                }
            };
        };

        // Create ValueFetcher from mocked DerivedFieldScript.Factory
        DerivedFieldScript.LeafFactory leafFactory = factory.newFactory((new Script("")).getParams(), searchLookup);
        Function<Object, IndexableField> indexableFieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
            "keyword",
            "ip_from_raw_request"
        );
        DerivedFieldValueFetcher valueFetcher = new DerivedFieldValueFetcher(leafFactory, null);

        // Create DerivedFieldQuery
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            new TermQuery(new Term("ip_from_raw_request", "247.37.0.0")),
            () -> valueFetcher,
            searchLookup,
            Lucene.STANDARD_ANALYZER,
            indexableFieldFunction,
            true
        );

        // Index and Search

        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(derivedFieldQuery, 10);
                assertEquals(3, topDocs.totalHits.value());
            }
        }
    }

    public void testDerivedFieldWithIgnoreMalformed() throws IOException {
        // Create lucene documents
        List<Document> docs = new ArrayList<>();
        for (String[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_request", request[0], Field.Store.YES));
            document.add(new KeywordField("status", request[1], Field.Store.YES));
            docs.add(document);
        }

        // Mock SearchLookup
        SearchLookup searchLookup = mock(SearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(leafLookup.source()).thenReturn(sourceLookup);

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> {
            when(searchLookup.getLeafSearchLookup(ctx)).thenReturn(leafLookup);
            return new DerivedFieldScript(params, lookup, ctx) {
                @Override
                public void execute() {
                    addEmittedValue(raw_requests[sourceLookup.docId()][2]);
                }
            };
        };

        // Create ValueFetcher from mocked DerivedFieldScript.Factory
        DerivedFieldScript.LeafFactory leafFactory = factory.newFactory((new Script("")).getParams(), searchLookup);
        Function<Object, IndexableField> badIndexableFieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
            "date",
            "ip_from_raw_request"
        );
        DerivedFieldValueFetcher valueFetcher = new DerivedFieldValueFetcher(leafFactory, null);
        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);
                // Create DerivedFieldQuery
                DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
                    new TermQuery(new Term("ip_from_raw_request", "247.37.0.0")),
                    () -> valueFetcher,
                    searchLookup,
                    Lucene.STANDARD_ANALYZER,
                    badIndexableFieldFunction,
                    false
                );
                DerivedFieldQuery finalDerivedFieldQuery = derivedFieldQuery;
                assertThrows(ClassCastException.class, () -> searcher.search(finalDerivedFieldQuery, 10));

                // set ignore_malformed as true, query should pass
                derivedFieldQuery = new DerivedFieldQuery(
                    new TermQuery(new Term("ip_from_raw_request", "247.37.0.0")),
                    () -> valueFetcher,
                    searchLookup,
                    Lucene.STANDARD_ANALYZER,
                    badIndexableFieldFunction,
                    true
                );
                searcher.search(derivedFieldQuery, 10);
                TopDocs topDocs = searcher.search(derivedFieldQuery, 10);
                assertEquals(0, topDocs.totalHits.value());
            }
        }
    }

    public void testNeedsRewriteWithPointRangeQuery() throws IOException {
        // Create lucene documents
        List<Document> docs = new ArrayList<>();
        for (String[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_request", request[0], Field.Store.YES));
            document.add(new KeywordField("status", request[1], Field.Store.YES));
            docs.add(document);
        }

        // Mock SearchLookup
        SearchLookup searchLookup = mock(SearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(leafLookup.source()).thenReturn(sourceLookup);

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> {
            when(searchLookup.getLeafSearchLookup(ctx)).thenReturn(leafLookup);
            return new DerivedFieldScript(params, lookup, ctx) {
                @Override
                public void execute() {
                    addEmittedValue(raw_requests[sourceLookup.docId()][2]);
                }
            };
        };

        // Create ValueFetcher from mocked DerivedFieldScript.Factory
        DerivedFieldScript.LeafFactory leafFactory = factory.newFactory((new Script("")).getParams(), searchLookup);
        Function<Object, IndexableField> indexableFieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
            "keyword",
            "ip_from_raw_request"
        );
        DerivedFieldValueFetcher valueFetcher = new DerivedFieldValueFetcher(leafFactory, null);

        // Create a real PointRangeQuery using IntPoint
        Query pointRangeQuery = IntPoint.newRangeQuery("test_field", 0, 100);

        // Create DerivedFieldQuery with PointRangeQuery
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            pointRangeQuery,
            () -> valueFetcher,
            searchLookup,
            Lucene.STANDARD_ANALYZER,
            indexableFieldFunction,
            true
        );

        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);

                // Rewrite should return the same query without attempting rewrite
                Query rewritten = derivedFieldQuery.rewrite(searcher);
                assertSame(derivedFieldQuery, rewritten);
            }
        }
    }

    public void testNeedsRewriteWithIndexOrDocValuesQueryAndPointRange() throws IOException {
        // Create lucene documents
        List<Document> docs = new ArrayList<>();
        for (String[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_request", request[0], Field.Store.YES));
            document.add(new KeywordField("status", request[1], Field.Store.YES));
            docs.add(document);
        }

        // Mock SearchLookup
        SearchLookup searchLookup = mock(SearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(leafLookup.source()).thenReturn(sourceLookup);

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> {
            when(searchLookup.getLeafSearchLookup(ctx)).thenReturn(leafLookup);
            return new DerivedFieldScript(params, lookup, ctx) {
                @Override
                public void execute() {
                    addEmittedValue(raw_requests[sourceLookup.docId()][2]);
                }
            };
        };

        // Create ValueFetcher from mocked DerivedFieldScript.Factory
        DerivedFieldScript.LeafFactory leafFactory = factory.newFactory((new Script("")).getParams(), searchLookup);
        Function<Object, IndexableField> indexableFieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
            "keyword",
            "ip_from_raw_request"
        );
        DerivedFieldValueFetcher valueFetcher = new DerivedFieldValueFetcher(leafFactory, null);

        // Create real queries: PointRangeQuery -> IndexOrDocValuesQuery
        Query pointRangeQuery = IntPoint.newRangeQuery("test_field", 0, 100);
        Query dvQuery = new MatchNoDocsQuery();
        IndexOrDocValuesQuery indexOrDocValuesQuery = new IndexOrDocValuesQuery(pointRangeQuery, dvQuery);

        // Create DerivedFieldQuery with IndexOrDocValuesQuery with PointRangeQuery
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            indexOrDocValuesQuery,
            () -> valueFetcher,
            searchLookup,
            Lucene.STANDARD_ANALYZER,
            indexableFieldFunction,
            true
        );

        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);

                // Rewrite should return the same query without attempting rewrite
                Query rewritten = derivedFieldQuery.rewrite(searcher);
                assertSame(derivedFieldQuery, rewritten);
            }
        }
    }

    public void testNeedsRewriteWithRegularQuery() throws IOException {
        // Create lucene documents
        List<Document> docs = new ArrayList<>();
        for (String[] request : raw_requests) {
            Document document = new Document();
            document.add(new TextField("raw_request", request[0], Field.Store.YES));
            document.add(new KeywordField("status", request[1], Field.Store.YES));
            docs.add(document);
        }

        // Mock SearchLookup
        SearchLookup searchLookup = mock(SearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(leafLookup.source()).thenReturn(sourceLookup);

        // Mock DerivedFieldScript.Factory
        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> {
            when(searchLookup.getLeafSearchLookup(ctx)).thenReturn(leafLookup);
            return new DerivedFieldScript(params, lookup, ctx) {
                @Override
                public void execute() {
                    addEmittedValue(raw_requests[sourceLookup.docId()][2]);
                }
            };
        };

        // Create ValueFetcher from mocked DerivedFieldScript.Factory
        DerivedFieldScript.LeafFactory leafFactory = factory.newFactory((new Script("")).getParams(), searchLookup);
        Function<Object, IndexableField> indexableFieldFunction = DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(
            "keyword",
            "ip_from_raw_request"
        );
        DerivedFieldValueFetcher valueFetcher = new DerivedFieldValueFetcher(leafFactory, null);

        // Create DerivedFieldQuery with regular TermQuery
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            new TermQuery(new Term("ip_from_raw_request", "247.37.0.0")),
            () -> valueFetcher,
            searchLookup,
            Lucene.STANDARD_ANALYZER,
            indexableFieldFunction,
            true
        );

        // Index and Search
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            for (Document d : docs) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);

                // Rewrite should perform rewrite on the inner query (TermQuery doesn't change in this case)
                Query rewritten = derivedFieldQuery.rewrite(searcher);
                // Since TermQuery doesn't rewrite to something different, we still get the same DerivedFieldQuery back
                assertSame(derivedFieldQuery, rewritten);
            }
        }
    }

    /**
     * A derived field whose script fails on docs that a cheaper filter clause already excludes must not
     * be evaluated on those docs. Sorting on a field skips scoring, so the conjunction runs through
     * Lucene's dense bulk scorer, which confirmed two-phase clauses on every doc of a window before
     * Lucene 10.6. See https://github.com/opensearch-project/OpenSearch/issues/23432
     */
    public void testDerivedFieldNotEvaluatedOnDocsExcludedByFilter() throws IOException {
        int numDocs = 10_000;
        // Every 4th doc is invalid and has no expiry value, so ~75% of docs pass the filter.
        IntPredicate isValid = docId -> docId % 4 != 0;

        SearchLookup searchLookup = mock(SearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(leafLookup.source()).thenReturn(sourceLookup);

        DerivedFieldScript.Factory factory = (params, lookup) -> (DerivedFieldScript.LeafFactory) ctx -> {
            when(searchLookup.getLeafSearchLookup(ctx)).thenReturn(leafLookup);
            return new DerivedFieldScript(params, lookup, ctx) {
                @Override
                public void execute() {
                    if (isValid.test(sourceLookup.docId()) == false) {
                        throw new IllegalStateException("A document doesn't have a value for a field!");
                    }
                    addEmittedValue("valid");
                }
            };
        };
        DerivedFieldScript.LeafFactory leafFactory = factory.newFactory((new Script("")).getParams(), searchLookup);
        DerivedFieldValueFetcher valueFetcher = new DerivedFieldValueFetcher(leafFactory, null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            new TermQuery(new Term("derived", "valid")),
            () -> valueFetcher,
            searchLookup,
            Lucene.STANDARD_ANALYZER,
            DerivedFieldSupportedTypes.getIndexableFieldGeneratorType("keyword", "derived"),
            false
        );
        Query query = new BooleanQuery.Builder().add(new TermQuery(new Term("is_valid", "true")), BooleanClause.Occur.FILTER)
            .add(derivedFieldQuery, BooleanClause.Occur.FILTER)
            .build();

        try (Directory dir = newDirectory()) {
            // No merges, so all docs land in one segment in insertion order and doc ids match isValid.
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE));
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new KeywordField("is_valid", Boolean.toString(isValid.test(i)), Field.Store.NO));
                if (isValid.test(i)) {
                    document.add(new NumericDocValuesField("expiry", i));
                }
                iw.addDocument(document);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(query, 10, new Sort(new SortField("expiry", SortField.Type.LONG, true)));
                assertEquals(numDocs * 3 / 4, topDocs.totalHits.value());
            }
        }
    }
}
