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

package org.opensearch.painless;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.similarity.ScriptedSimilarity;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.SimilarityScript;
import org.opensearch.script.SimilarityWeightScript;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimilarityScriptTests extends ScriptTestCase {
    private static PainlessScriptEngine SCRIPT_ENGINE;

    @BeforeClass
    public static void beforeClass() {
        Map<ScriptContext<?>, List<Allowlist>> contexts = new HashMap<>();
        contexts.put(SimilarityScript.CONTEXT, Allowlist.BASE_ALLOWLISTS);
        contexts.put(SimilarityWeightScript.CONTEXT, Allowlist.BASE_ALLOWLISTS);
        SCRIPT_ENGINE = new PainlessScriptEngine(Settings.EMPTY, contexts);
    }

    @AfterClass
    public static void afterClass() {
        SCRIPT_ENGINE = null;
    }

    @Override
    protected PainlessScriptEngine getEngine() {
        return SCRIPT_ENGINE;
    }

    public void testBasics() throws IOException {
        SimilarityScript.Factory factory = getEngine().compile(
            "foobar",
            "return query.boost * doc.freq / doc.length",
            SimilarityScript.CONTEXT,
            Collections.emptyMap()
        );
        ScriptedSimilarity sim = new ScriptedSimilarity("foobar", null, "foobaz", factory::newInstance, true);
        Directory dir = new ByteBuffersDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(sim));

        Document doc = new Document();
        doc.add(new TextField("f", "foo bar", Store.NO));
        doc.add(new StringField("match", "no", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "foo foo bar", Store.NO));
        doc.add(new StringField("match", "yes", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "bar", Store.NO));
        doc.add(new StringField("match", "no", Store.NO));
        w.addDocument(doc);

        IndexReader r = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = new IndexSearcher(r);
        searcher.setSimilarity(sim);
        Query query = new BoostQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("f", "foo")), Occur.SHOULD)
                .add(new TermQuery(new Term("match", "yes")), Occur.FILTER)
                .build(),
            3.2f
        );
        TopDocs topDocs = searcher.search(query, 1);
        assertEquals(1, topDocs.totalHits.value());
        assertEquals((float) (3.2 * 2 / 3), topDocs.scoreDocs[0].score, 0);
        w.close();
        dir.close();
    }

    public void testWeightScript() throws IOException {
        SimilarityWeightScript.Factory weightFactory = getEngine().compile(
            "foobar",
            "return query.boost",
            SimilarityWeightScript.CONTEXT,
            Collections.emptyMap()
        );
        SimilarityScript.Factory factory = getEngine().compile(
            "foobar",
            "return weight * doc.freq / doc.length",
            SimilarityScript.CONTEXT,
            Collections.emptyMap()
        );
        ScriptedSimilarity sim = new ScriptedSimilarity("foobar", weightFactory::newInstance, "foobaz", factory::newInstance, true);
        Directory dir = new ByteBuffersDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(sim));

        Document doc = new Document();
        doc.add(new TextField("f", "foo bar", Store.NO));
        doc.add(new StringField("match", "no", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "foo foo bar", Store.NO));
        doc.add(new StringField("match", "yes", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "bar", Store.NO));
        doc.add(new StringField("match", "no", Store.NO));
        w.addDocument(doc);

        IndexReader r = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = new IndexSearcher(r);
        searcher.setSimilarity(sim);
        Query query = new BoostQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("f", "foo")), Occur.SHOULD)
                .add(new TermQuery(new Term("match", "yes")), Occur.FILTER)
                .build(),
            3.2f
        );
        TopDocs topDocs = searcher.search(query, 1);
        assertEquals(1, topDocs.totalHits.value());
        assertEquals((float) (3.2 * 2 / 3), topDocs.scoreDocs[0].score, 0);
        w.close();
        dir.close();
    }
}
