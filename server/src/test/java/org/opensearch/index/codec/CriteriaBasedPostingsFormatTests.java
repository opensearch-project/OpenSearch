/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class CriteriaBasedPostingsFormatTests extends BasePostingsFormatTestCase {

    private static final String TEST_BUCKET = "test_bucket";
    private Codec criteriaBasedPostingFormat = TestUtil.alwaysPostingsFormat(
        new CriteriaBasedPostingsFormat(TestUtil.getDefaultPostingsFormat(), TEST_BUCKET)
    );

    public void testBasicFunctionality() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(criteriaBasedPostingFormat);

        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new StringField("_id", "doc" + i, Field.Store.YES));
                doc.add(new TextField("field", "value" + i, Field.Store.YES));
                writer.addDocument(doc);
            }
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
            assertEquals(100, reader.numDocs());

            Terms terms = MultiTerms.getTerms(reader, "field");
            assertNotNull(terms);
            TermsEnum termsEnum = terms.iterator();

            int count = 0;
            BytesRef term;
            while ((term = termsEnum.next()) != null) {
                assertTrue(term.utf8ToString().startsWith("value"));
                count++;
            }
            assertEquals(100, count);
        }
    }

    public void testBucketAttributeIsSetOnSegment() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(criteriaBasedPostingFormat);

        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc1", Field.Store.YES));
            doc.add(new TextField("content", "test content", Field.Store.YES));
            writer.addDocument(doc);
        }

        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(dir);
        assertFalse("Should have at least one segment", segmentInfos.asList().isEmpty());

        for (SegmentCommitInfo segmentCommitInfo : segmentInfos) {
            String bucketValue = segmentCommitInfo.info.getAttribute(CriteriaBasedPostingsFormat.BUCKET_NAME);
            assertEquals("Bucket attribute should be set", TEST_BUCKET, bucketValue);
        }
    }

    public void testNullBucketSetsPlaceholder() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        Codec nullBucketCodec = TestUtil.alwaysPostingsFormat(new CriteriaBasedPostingsFormat(TestUtil.getDefaultPostingsFormat(), null));

        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(nullBucketCodec);

        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc1", Field.Store.YES));
            doc.add(new TextField("content", "test content", Field.Store.YES));
            writer.addDocument(doc);
        }

        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(dir);
        for (SegmentCommitInfo segmentCommitInfo : segmentInfos) {
            String bucketValue = segmentCommitInfo.info.getAttribute(CriteriaBasedPostingsFormat.BUCKET_NAME);
            assertEquals("Placeholder bucket should be set for null bucket", "-2", bucketValue);
        }
    }

    public void testEmptyIndex() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(criteriaBasedPostingFormat);

        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            // Don't add any documents
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
            assertEquals(0, reader.numDocs());
            Terms terms = MultiTerms.getTerms(reader, "_id");
            assertNull("Terms should be null for empty index", terms);
        }
    }

    @Override
    protected Codec getCodec() {
        return criteriaBasedPostingFormat;
    }
}
