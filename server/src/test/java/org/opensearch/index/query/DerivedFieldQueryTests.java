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
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
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
                assertEquals(3, topDocs.totalHits.value);
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
                assertEquals(0, topDocs.totalHits.value);
            }
        }
    }
}
