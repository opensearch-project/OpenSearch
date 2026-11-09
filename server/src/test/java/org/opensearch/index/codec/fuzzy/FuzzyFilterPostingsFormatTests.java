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
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")
public class FuzzyFilterPostingsFormatTests extends BasePostingsFormatTestCase {
    private TreeMap<String, FuzzySetParameters> params = new TreeMap<>() {
        @Override
        public FuzzySetParameters get(Object k) {
            return new FuzzySetParameters(() -> FuzzySetParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY);
        }
    };

    private Codec fuzzyFilterCodec = TestUtil.alwaysPostingsFormat(
        new FuzzyFilterPostingsFormat(TestUtil.getDefaultPostingsFormat(), new FuzzySetFactory(params))
    );

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
                assertTrue("Should find existing term", termsEnum.seekExact(new BytesRef("value" + i)));
            }

            for (int i = 1000; i < 2000; i++) {
                assertFalse("Should not find non-existent term", termsEnum.seekExact(new BytesRef("value" + i)));
            }
        }
    }

    public void testFuzzyFilterWithDifferentFieldTypes() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(fuzzyFilterCodec);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc1", Field.Store.YES));
            doc.add(new TextField("text_field", "some analyzed text", Field.Store.YES));
            doc.add(new StringField("keyword_field", "exact_match", Field.Store.YES));
            doc.add(new StringField("date_field", "2025-10-16", Field.Store.YES));
            doc.add(new IntPoint("int_field", 42));
            doc.add(new LongPoint("long_field", 42L));
            doc.add(new DoublePoint("double_field", 42.0));
            doc.add(new BinaryDocValuesField("binary_field", new BytesRef("binary_data")));
            writer.addDocument(doc);
        }
        try (IndexReader reader = DirectoryReader.open(dir)) {
            for (String fieldName : Arrays.asList("_id", "text_field", "keyword_field", "date_field")) {
                Terms terms = MultiTerms.getTerms(reader, fieldName);
                assertNotNull("Should have terms for " + fieldName, terms);
                TermsEnum termsEnum = terms.iterator();
                assertTrue("Should find existing term in " + fieldName, termsEnum.seekExact(new BytesRef(getTestValueForField(fieldName))));
                assertFalse(
                    "Should not find non-existent term in " + fieldName,
                    termsEnum.seekExact(new BytesRef("nonexistent_" + fieldName))
                );
            }
            for (String fieldName : Arrays.asList("int_field", "long_field", "double_field", "binary_field")) {
                Terms terms = MultiTerms.getTerms(reader, fieldName);
                assertNull("Should not have terms for " + fieldName, terms);
            }
        }
    }

    private String getTestValueForField(String fieldName) {
        switch (fieldName) {
            case "_id":
                return "doc1";
            case "text_field":
                return "text";
            case "keyword_field":
                return "exact_match";
            case "int_field":
                return "42";
            case "long_field":
                return "42";
            case "double_field":
                return "42.0";
            case "date_field":
                return "2025-10-16";
            case "binary_field":
                return "binary_data";
            default:
                return "";
        }
    }

    public void testIDandTextFields() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(fuzzyFilterCodec);

        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new StringField("id_field", "ID_" + i, Field.Store.YES));
                doc.add(new TextField("text_field", "value" + i, Field.Store.YES));
                writer.addDocument(doc);
            }
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
            assertEquals(100, reader.numDocs());
            Terms idTerms = MultiTerms.getTerms(reader, "id_field");
            assertNotNull("Terms should exist for id_field", idTerms);
            TermsEnum idTermsEnum = idTerms.iterator();
            int idCount = 0;
            while (idTermsEnum.next() != null) {
                idCount++;
            }
            assertEquals("ID field should have 100 unique terms", 100, idCount);
            idTermsEnum = idTerms.iterator();
            assertTrue("Should find existing ID", idTermsEnum.seekExact(new BytesRef("ID_50")));
            assertFalse("Should not find non-existent ID", idTermsEnum.seekExact(new BytesRef("ID_nonexistent")));
            Terms textTerms = MultiTerms.getTerms(reader, "text_field");
            assertNotNull("Terms should exist for text_field", textTerms);
            TermsEnum textTermsEnum = textTerms.iterator();
            int textCount = 0;
            while (textTermsEnum.next() != null) {
                textCount++;
            }
            assertEquals("Text field should have 100 unique terms", 100, textCount);
            textTermsEnum = textTerms.iterator();
            assertTrue("Should find existing text", textTermsEnum.seekExact(new BytesRef("value50")));
            assertFalse("Should not find non-existent text", textTermsEnum.seekExact(new BytesRef("valuenonexistent")));
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
