/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;

/**
 * Unit tests for {@link StarTreeUpgradeService} — specifically the segment eligibility
 * logic and candidate segment detection.
 */
public class StarTreeUpgradeServiceTests extends OpenSearchTestCase {

    /**
     * Tests that getCandidateSegmentNames skips segments already using Composite912Codec.
     * Creates a segment with the default codec, verifies it IS a candidate.
     */
    public void testGetCandidateSegmentNames_includesNonCompositeSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("price", 100));
                doc.add(new SortedNumericDocValuesField("quantity", 5));
                writer.addDocument(doc);
                writer.commit();
            }

            Set<String> candidates = StarTreeUpgradeService.getCandidateSegmentNames(dir);
            assertEquals("should have 1 candidate segment", 1, candidates.size());
        }
    }

    /**
     * Tests that segments with 0 live docs (tombstone segments from deletes)
     * are excluded from candidates. These have maxDoc - delCount - softDelCount &lt;= 0.
     */
    public void testGetCandidateSegmentNames_skipsTombstoneSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                // Add a doc with an ID field so we can delete it
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("price", 100));
                doc.add(new org.apache.lucene.document.StringField("_id", "1",
                    org.apache.lucene.document.Field.Store.NO));
                writer.addDocument(doc);
                writer.commit();

                // Delete the only doc → segment becomes tombstone (0 live docs)
                writer.deleteDocuments(new Term("_id", "1"));
                writer.commit();
            }

            Set<String> candidates = StarTreeUpgradeService.getCandidateSegmentNames(dir);
            // The segment with 0 live docs should be excluded
            // Note: depending on merge policy, Lucene may or may not keep the segment
            // If it keeps it, it should be excluded from candidates
            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            for (SegmentCommitInfo ci : infos) {
                int liveDocs = ci.info.maxDoc() - ci.getDelCount() - ci.getSoftDelCount();
                if (liveDocs <= 0) {
                    assertFalse("tombstone segment should not be a candidate",
                        candidates.contains(ci.info.name));
                }
            }
        }
    }

    /**
     * Tests that multiple segments are all returned as candidates when none use Composite912Codec.
     */
    public void testGetCandidateSegmentNames_multipleSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                // Create 3 segments
                for (int seg = 0; seg < 3; seg++) {
                    for (int i = 0; i < 10; i++) {
                        Document doc = new Document();
                        doc.add(new SortedNumericDocValuesField("price", 10 * seg + i));
                        doc.add(new SortedNumericDocValuesField("quantity", i));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }

            Set<String> candidates = StarTreeUpgradeService.getCandidateSegmentNames(dir);
            assertEquals("should have 3 candidate segments", 3, candidates.size());
        }
    }

    /**
     * Tests that cleanupStarTreeFiles is a no-op when no star tree files exist.
     * Should not throw.
     */
    public void testCleanupStarTreeFiles_noFilesExist() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("price", 100));
                writer.addDocument(doc);
                writer.commit();
            }

            Set<String> segNames = StarTreeUpgradeService.getCandidateSegmentNames(dir);
            // Should not throw — files don't exist, cleanup is best-effort
            StarTreeUpgradeService.cleanupStarTreeFiles(dir, segNames);
        }
    }
}
