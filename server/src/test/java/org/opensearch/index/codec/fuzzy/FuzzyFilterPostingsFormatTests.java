/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.TreeMap;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")
public class FuzzyFilterPostingsFormatTests extends BasePostingsFormatTestCase {
    private TreeMap<String, FuzzySetParameters> params = new TreeMap<>() {
        @Override
        public FuzzySetParameters get(Object k) {
            return new FuzzySetParameters(() -> FuzzySetParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY);
        }
    };

    private Codec fuzzyFilterCodec = TestUtil.alwaysPostingsFormat(new FuzzyFilterPostingsFormat(TestUtil.getDefaultPostingsFormat(), new FuzzySetFactory(params)));

    @Override
    protected Codec getCodec() {
        return fuzzyFilterCodec;
    }

    public void testBasicFunctionality() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(fuzzyFilterCodec);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new TextField("field", "value" + i, Field.Store.YES));
                writer.addDocument(doc);
            }
        }
        try (IndexReader reader = DirectoryReader.open(dir)) {
            assertEquals(100, reader.numDocs());
            Terms terms = MultiTerms.getTerms(reader, "field");
            TermsEnum termsEnum = terms.iterator();

            BytesRef term;
            int count = 0;
            while ((term = termsEnum.next()) != null) {
                assertTrue(term.utf8ToString().startsWith("value"));
                count++;
            }
            assertEquals(100, count);
        }
    }

    public void testBloomFilterFunctionality() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(fuzzyFilterCodec);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < 1000; i++) {
                Document doc = new Document();
                doc.add(new TextField("field", "value" + i, Field.Store.YES));
                writer.addDocument(doc);
            }
        }
        try (IndexReader reader = DirectoryReader.open(dir)) {
            Terms terms = MultiTerms.getTerms(reader, "field");
            TermsEnum termsEnum = terms.iterator();
            for (int i = 0; i < 1000; i++) {
                assertTrue(termsEnum.seekExact(new BytesRef("value" + i)));
            }
            int falsePositives = 0;
            for (int i = 1000; i < 2000; i++) {
                if (termsEnum.seekExact(new BytesRef("value" + i))) {
                    falsePositives++;
                }
            }
            double falsePositiveRate = (double) falsePositives / 1000;
            assertTrue("False positive rate too high: " + falsePositiveRate, falsePositiveRate < 0.01);
        }
    }

    public void testMultipleFields() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(fuzzyFilterCodec);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new TextField("field1", "value" + i, Field.Store.YES));
                doc.add(new TextField("field2", "other" + i, Field.Store.YES));
                writer.addDocument(doc);
            }
        }
        try (IndexReader reader = DirectoryReader.open(dir)) {
            assertEquals(100, reader.numDocs());
            Terms terms1 = MultiTerms.getTerms(reader, "field1");
            TermsEnum termsEnum1 = terms1.iterator();
            int count1 = 0;
            while (termsEnum1.next() != null) {
                count1++;
            }
            assertEquals(100, count1);
            Terms terms2 = MultiTerms.getTerms(reader, "field2");
            TermsEnum termsEnum2 = terms2.iterator();
            int count2 = 0;
            while (termsEnum2.next() != null) {
                count2++;
            }
            assertEquals(100, count2);
        }
    }

    public void testLargeNumberOfTerms() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(fuzzyFilterCodec);
        int numTerms = 100000;
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numTerms; i++) {
                Document doc = new Document();
                doc.add(new TextField("field", "unique" + i, Field.Store.NO));
                writer.addDocument(doc);
            }
        }
        try (IndexReader reader = DirectoryReader.open(dir)) {
            assertEquals(numTerms, reader.numDocs());
            Terms terms = MultiTerms.getTerms(reader, "field");
            TermsEnum termsEnum = terms.iterator();

            int count = 0;
            while (termsEnum.next() != null) {
                count++;
            }
            assertEquals(numTerms, count);
            int falsePositives = 0;
            for (int i = numTerms; i < numTerms + 1000; i++) {
                if (termsEnum.seekExact(new BytesRef("unique" + i))) {
                    falsePositives++;
                }
            }
            double falsePositiveRate = (double) falsePositives / 1000;
            assertTrue("False positive rate too high: " + falsePositiveRate, falsePositiveRate < 0.05);
        }
    }
}
