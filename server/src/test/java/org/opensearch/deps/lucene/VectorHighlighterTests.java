/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.deps.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.lucene.search.vectorhighlight.CustomFieldQuery;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class VectorHighlighterTests extends OpenSearchTestCase {
    public void testVectorHighlighter() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        FieldType vectorsType = new FieldType(TextField.TYPE_STORED);
        vectorsType.setStoreTermVectors(true);
        vectorsType.setStoreTermVectorPositions(true);
        vectorsType.setStoreTermVectorOffsets(true);
        document.add(new Field("content", "the big bad dog", vectorsType));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits.value(), equalTo(1L));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(
            highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
            reader,
            topDocs.scoreDocs[0].doc,
            "content",
            30
        );
        assertThat(fragment, notNullValue());
        assertThat(fragment, equalTo("the big <b>bad</b> dog"));
    }

    public void testVectorHighlighterPrefixQuery() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        FieldType vectorsType = new FieldType(TextField.TYPE_STORED);
        vectorsType.setStoreTermVectors(true);
        vectorsType.setStoreTermVectorPositions(true);
        vectorsType.setStoreTermVectorOffsets(true);
        document.add(new Field("content", "the big bad dog", vectorsType));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits.value(), equalTo(1L));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();

        PrefixQuery prefixQuery = new PrefixQuery(new Term("content", "ba"));
        assertThat(
            prefixQuery.getRewriteMethod().getClass().getName(),
            equalTo(PrefixQuery.CONSTANT_SCORE_BLENDED_REWRITE.getClass().getName())
        );
        String fragment = highlighter.getBestFragment(
            highlighter.getFieldQuery(prefixQuery),
            reader,
            topDocs.scoreDocs[0].doc,
            "content",
            30
        );
        assertThat(fragment, nullValue());

        prefixQuery = new PrefixQuery(new Term("content", "ba"), PrefixQuery.SCORING_BOOLEAN_REWRITE);
        Query rewriteQuery = prefixQuery.rewrite(searcher);
        fragment = highlighter.getBestFragment(highlighter.getFieldQuery(rewriteQuery), reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, notNullValue());

        // now check with the custom field query
        prefixQuery = new PrefixQuery(new Term("content", "ba"));
        assertThat(
            prefixQuery.getRewriteMethod().getClass().getName(),
            equalTo(PrefixQuery.CONSTANT_SCORE_BLENDED_REWRITE.getClass().getName())
        );
        fragment = highlighter.getBestFragment(
            new CustomFieldQuery(prefixQuery, reader, highlighter),
            reader,
            topDocs.scoreDocs[0].doc,
            "content",
            30
        );
        assertThat(fragment, notNullValue());
    }

    public void testVectorHighlighterNoStore() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        FieldType vectorsType = new FieldType(TextField.TYPE_NOT_STORED);
        vectorsType.setStoreTermVectors(true);
        vectorsType.setStoreTermVectorPositions(true);
        vectorsType.setStoreTermVectorOffsets(true);
        document.add(new Field("content", "the big bad dog", vectorsType));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits.value(), equalTo(1L));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(
            highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
            reader,
            topDocs.scoreDocs[0].doc,
            "content",
            30
        );
        assertThat(fragment, nullValue());
    }

    public void testVectorHighlighterNoTermVector() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new TextField("content", "the big bad dog", Field.Store.YES));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits.value(), equalTo(1L));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(
            highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
            reader,
            topDocs.scoreDocs[0].doc,
            "content",
            30
        );
        assertThat(fragment, nullValue());
    }
}
