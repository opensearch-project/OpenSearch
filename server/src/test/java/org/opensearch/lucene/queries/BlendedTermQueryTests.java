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

package org.opensearch.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.search.QueryUtils;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.EqualsHashCodeTestUtils.CopyFunction;
import org.opensearch.test.EqualsHashCodeTestUtils.MutateFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class BlendedTermQueryTests extends OpenSearchTestCase {
    public void testDismaxQuery() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        String[] username = new String[] { "foo fighters", "some cool fan", "cover band" };
        String[] song = new String[] { "generator", "foo fighers - generator", "foo fighters generator" };
        final boolean omitNorms = random().nextBoolean();
        final boolean omitFreqs = random().nextBoolean();
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(omitFreqs ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(omitNorms);
        ft.freeze();

        for (int i = 0; i < username.length; i++) {
            Document d = new Document();
            d.add(new TextField("id", Integer.toString(i), Field.Store.YES));
            d.add(new Field("username", username[i], ft));
            d.add(new Field("song", song[i], ft));
            w.addDocument(d);
        }
        int iters = scaledRandomIntBetween(25, 100);
        for (int j = 0; j < iters; j++) {
            Document d = new Document();
            d.add(new TextField("id", Integer.toString(username.length + j), Field.Store.YES));
            d.add(new Field("username", "foo fighters", ft));
            d.add(new Field("song", "some bogus text to bump up IDF", ft));
            w.addDocument(d);
        }
        w.commit();
        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = setSimilarity(newSearcher(reader));
        {
            String[] fields = new String[] { "username", "song" };
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.add(BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "foo"), 0.1f), BooleanClause.Occur.SHOULD);
            query.add(BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "fighters"), 0.1f), BooleanClause.Occur.SHOULD);
            query.add(BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "generator"), 0.1f), BooleanClause.Occur.SHOULD);
            TopDocs search = searcher.search(query.build(), 10);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            assertEquals(Integer.toString(0), reader.storedFields().document(scoreDocs[0].doc).getField("id").stringValue());
        }
        {
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            DisjunctionMaxQuery uname = new DisjunctionMaxQuery(
                Arrays.asList(new TermQuery(new Term("username", "foo")), new TermQuery(new Term("song", "foo"))),
                0.0f
            );

            DisjunctionMaxQuery s = new DisjunctionMaxQuery(
                Arrays.asList(new TermQuery(new Term("username", "fighers")), new TermQuery(new Term("song", "fighers"))),
                0.0f
            );
            DisjunctionMaxQuery gen = new DisjunctionMaxQuery(
                Arrays.asList(new TermQuery(new Term("username", "generator")), new TermQuery(new Term("song", "generator"))),
                0f
            );
            query.add(uname, BooleanClause.Occur.SHOULD);
            query.add(s, BooleanClause.Occur.SHOULD);
            query.add(gen, BooleanClause.Occur.SHOULD);
            TopDocs search = searcher.search(query.build(), 4);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            assertEquals(Integer.toString(1), reader.storedFields().document(scoreDocs[0].doc).getField("id").stringValue());

        }
        {
            // test with an unknown field
            String[] fields = new String[] { "username", "song", "unknown_field" };
            Query query = BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "foo"), 1.0f);
            Query rewrite = searcher.rewrite(query);
            assertThat(rewrite, instanceOf(BooleanQuery.class));
            for (BooleanClause clause : (BooleanQuery) rewrite) {
                assertThat(clause.query(), instanceOf(TermQuery.class));
                TermQuery termQuery = (TermQuery) clause.query();
                TermStates termStates = termQuery.getTermStates();
                if (termQuery.getTerm().field().equals("unknown_field")) {
                    assertThat(termStates.docFreq(), equalTo(0));
                    assertThat(termStates.totalTermFreq(), equalTo(0L));
                } else {
                    assertThat(termStates.docFreq(), greaterThan(0));
                    assertThat(termStates.totalTermFreq(), greaterThan(0L));
                }
            }
            assertThat(searcher.search(query, 10).totalHits.value(), equalTo((long) iters + username.length));
        }
        {
            // test with an unknown field and an unknown term
            String[] fields = new String[] { "username", "song", "unknown_field" };
            Query query = BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "unknown_term"), 1.0f);
            Query rewrite = searcher.rewrite(query);
            assertThat(rewrite, instanceOf(BooleanQuery.class));
            for (BooleanClause clause : (BooleanQuery) rewrite) {
                assertThat(clause.query(), instanceOf(TermQuery.class));
                TermQuery termQuery = (TermQuery) clause.query();
                TermStates termStates = termQuery.getTermStates();
                assertThat(termStates.docFreq(), equalTo(0));
                assertThat(termStates.totalTermFreq(), equalTo(0L));
            }
            assertThat(searcher.search(query, 10).totalHits.value(), equalTo(0L));
        }
        {
            // test with an unknown field and a term that is present in only one field
            String[] fields = new String[] { "username", "song", "id", "unknown_field" };
            Query query = BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "fan"), 1.0f);
            Query rewrite = searcher.rewrite(query);
            assertThat(rewrite, instanceOf(BooleanQuery.class));
            for (BooleanClause clause : (BooleanQuery) rewrite) {
                assertThat(clause.query(), instanceOf(TermQuery.class));
                TermQuery termQuery = (TermQuery) clause.query();
                TermStates termStates = termQuery.getTermStates();
                if (termQuery.getTerm().field().equals("username")) {
                    assertThat(termStates.docFreq(), equalTo(1));
                    assertThat(termStates.totalTermFreq(), equalTo(1L));
                } else {
                    assertThat(termStates.docFreq(), equalTo(0));
                    assertThat(termStates.totalTermFreq(), equalTo(0L));
                }
            }
            assertThat(searcher.search(query, 10).totalHits.value(), equalTo(1L));
        }
        reader.close();
        w.close();
        dir.close();
    }

    public void testBasics() {
        final int iters = scaledRandomIntBetween(5, 25);
        for (int j = 0; j < iters; j++) {
            String[] fields = new String[1 + random().nextInt(10)];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = randomRealisticUnicodeOfLengthBetween(1, 10);
            }
            String term = randomRealisticUnicodeOfLengthBetween(1, 10);
            Term[] terms = toTerms(fields, term);
            float tieBreaker = random().nextFloat();
            BlendedTermQuery query = BlendedTermQuery.dismaxBlendedQuery(terms, tieBreaker);
            QueryUtils.check(query);
            terms = toTerms(fields, term);
            BlendedTermQuery query2 = BlendedTermQuery.dismaxBlendedQuery(terms, tieBreaker);
            assertEquals(query, query2);
        }
    }

    public Term[] toTerms(String[] fields, String term) {
        Term[] terms = new Term[fields.length];
        List<String> fieldsList = Arrays.asList(fields);
        Collections.shuffle(fieldsList, random());
        fields = fieldsList.toArray(new String[0]);
        for (int i = 0; i < fields.length; i++) {
            terms[i] = new Term(fields[i], term);
        }
        return terms;
    }

    public IndexSearcher setSimilarity(IndexSearcher searcher) {
        Similarity similarity = random().nextBoolean() ? new BM25Similarity() : new ClassicSimilarity();
        searcher.setSimilarity(similarity);
        return searcher;
    }

    public void testExtractTerms() throws IOException {
        Set<Term> terms = new HashSet<>();
        int num = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < num; i++) {
            terms.add(new Term(randomRealisticUnicodeOfLengthBetween(1, 10), randomRealisticUnicodeOfLengthBetween(1, 10)));
        }

        BlendedTermQuery blendedTermQuery = BlendedTermQuery.dismaxBlendedQuery(terms.toArray(new Term[0]), random().nextFloat());
        Set<Term> extracted = new HashSet<>();
        blendedTermQuery.visit(QueryVisitor.termCollector(extracted));
        assertThat(extracted.size(), equalTo(terms.size()));
        assertThat(extracted, containsInAnyOrder(terms.toArray(new Term[0])));
    }

    public void testMinTTF() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        for (int i = 0; i < 10; i++) {
            Document d = new Document();
            d.add(new TextField("id", Integer.toString(i), Field.Store.YES));
            d.add(new Field("dense", "foo foo foo", ft));
            if (i % 10 == 0) {
                d.add(new Field("sparse", "foo", ft));
            }
            w.addDocument(d);
        }
        w.commit();
        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = setSimilarity(newSearcher(reader));
        {
            String[] fields = new String[] { "dense", "sparse" };
            Query query = BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "foo"), 0.1f);
            TopDocs search = searcher.search(query, 10);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            assertEquals(Integer.toString(0), reader.storedFields().document(scoreDocs[0].doc).getField("id").stringValue());
        }
        reader.close();
        w.close();
        dir.close();
    }

    public void testEqualsAndHash() {
        String[] fields = new String[1 + random().nextInt(10)];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = randomRealisticUnicodeOfLengthBetween(1, 10);
        }
        String term = randomRealisticUnicodeOfLengthBetween(1, 10);
        Term[] terms = toTerms(fields, term);
        float tieBreaker = randomFloat();
        final float[] boosts;
        if (randomBoolean()) {
            boosts = new float[terms.length];
            for (int i = 0; i < terms.length; i++) {
                boosts[i] = randomFloat();
            }
        } else {
            boosts = null;
        }

        BlendedTermQuery original = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, tieBreaker);
        CopyFunction<BlendedTermQuery> copyFunction = org -> {
            Term[] termsCopy = new Term[terms.length];
            System.arraycopy(terms, 0, termsCopy, 0, terms.length);

            float[] boostsCopy = null;
            if (boosts != null) {
                boostsCopy = new float[boosts.length];
                System.arraycopy(boosts, 0, boostsCopy, 0, terms.length);
            }
            if (randomBoolean() && terms.length > 1) {
                // if we swap two elements, the resulting query should still be regarded as equal
                int swapPos = randomIntBetween(1, terms.length - 1);

                Term swpTerm = termsCopy[0];
                termsCopy[0] = termsCopy[swapPos];
                termsCopy[swapPos] = swpTerm;

                if (boosts != null) {
                    float swpBoost = boostsCopy[0];
                    boostsCopy[0] = boostsCopy[swapPos];
                    boostsCopy[swapPos] = swpBoost;
                }
            }
            return BlendedTermQuery.dismaxBlendedQuery(termsCopy, boostsCopy, tieBreaker);
        };
        MutateFunction<BlendedTermQuery> mutateFunction = org -> {
            if (randomBoolean()) {
                Term[] termsCopy = new Term[terms.length];
                System.arraycopy(terms, 0, termsCopy, 0, terms.length);
                termsCopy[randomIntBetween(0, terms.length - 1)] = new Term(randomAlphaOfLength(10), randomAlphaOfLength(10));
                return BlendedTermQuery.dismaxBlendedQuery(termsCopy, boosts, tieBreaker);
            } else {
                float[] boostsCopy = null;
                if (boosts != null) {
                    boostsCopy = new float[boosts.length];
                    System.arraycopy(boosts, 0, boostsCopy, 0, terms.length);
                    boostsCopy[randomIntBetween(0, terms.length - 1)] = randomFloat();
                } else {
                    boostsCopy = new float[terms.length];
                    for (int i = 0; i < terms.length; i++) {
                        boostsCopy[i] = randomFloat();
                    }
                }
                return BlendedTermQuery.dismaxBlendedQuery(terms, boostsCopy, tieBreaker);
            }
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, copyFunction, mutateFunction);
    }
}
