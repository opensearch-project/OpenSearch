/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class PrecompiledAutomatonQueryTests extends OpenSearchTestCase {

    public void testQueryExecutionMatchesRegexpQuery() throws IOException {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        Document doc1 = new Document();
        doc1.add(new TextField("field", "apple", Field.Store.YES));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new TextField("field", "banana", Field.Store.YES));
        writer.addDocument(doc2);

        Document doc3 = new Document();
        doc3.add(new TextField("field", "apricot", Field.Store.YES));
        writer.addDocument(doc3);

        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = newSearcher(reader);

        String pattern = "a.*";
        RegExp regExp = new RegExp(pattern, RegExp.ALL, 0);
        CompiledAutomaton compiled = new CompiledAutomaton(
            Operations.determinize(regExp.toAutomaton(RegexpQuery.DEFAULT_PROVIDER), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
        );

        Term term = new Term("field", pattern);
        PrecompiledAutomatonQuery precompiledQuery = new PrecompiledAutomatonQuery(term, compiled);
        RegexpQuery standardQuery = new RegexpQuery(
            term,
            RegExp.ALL,
            0,
            RegexpQuery.DEFAULT_PROVIDER,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
        );

        TopDocs precompiledResults = searcher.search(precompiledQuery, 10);
        TopDocs standardResults = searcher.search(standardQuery, 10);

        assertEquals(2, precompiledResults.totalHits.value());
        assertEquals(standardResults.totalHits.value(), precompiledResults.totalHits.value());

        reader.close();
        dir.close();
    }

    public void testEqualsAndHashCode() {
        RegExp regExp = new RegExp("test.*", RegExp.ALL, 0);
        CompiledAutomaton compiled1 = new CompiledAutomaton(
            Operations.determinize(regExp.toAutomaton(RegexpQuery.DEFAULT_PROVIDER), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
        );
        CompiledAutomaton compiled2 = new CompiledAutomaton(
            Operations.determinize(regExp.toAutomaton(RegexpQuery.DEFAULT_PROVIDER), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
        );

        Term term1 = new Term("field", "test.*");
        Term term2 = new Term("field", "test.*");
        Term term3 = new Term("other_field", "test.*");

        PrecompiledAutomatonQuery q1 = new PrecompiledAutomatonQuery(term1, compiled1);
        PrecompiledAutomatonQuery q2 = new PrecompiledAutomatonQuery(term2, compiled1);
        PrecompiledAutomatonQuery q3 = new PrecompiledAutomatonQuery(term3, compiled1);
        PrecompiledAutomatonQuery q4 = new PrecompiledAutomatonQuery(term1, compiled2);

        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertNotEquals(q1, q3);
        assertNotEquals(q1, q4);
    }

    public void testToString() {
        RegExp regExp = new RegExp("abc.*", RegExp.ALL, 0);
        CompiledAutomaton compiled = new CompiledAutomaton(
            Operations.determinize(regExp.toAutomaton(RegexpQuery.DEFAULT_PROVIDER), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
        );
        Term term = new Term("myfield", "abc.*");
        PrecompiledAutomatonQuery query = new PrecompiledAutomatonQuery(term, compiled);

        assertEquals("myfield:PrecompiledAutomatonQuery: abc.*", query.toString());
        assertEquals("PrecompiledAutomatonQuery: abc.*", query.toString("myfield"));
    }
}
