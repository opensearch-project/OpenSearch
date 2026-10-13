/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.backward_codecs.lucene100.Lucene100Codec;
import org.apache.lucene.backward_codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat.Mode;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.opensearch.index.codec.composite.composite104.Composite104Codec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

import org.mockito.Mockito;

import static org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat.Mode.BEST_COMPRESSION;
import static org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat.Mode.BEST_SPEED;

public class OpenSearchMergePolicyTests extends OpenSearchTestCase {
    public void testShouldUpgradeVersions() {
        var policy = new OpenSearchMergePolicy(NoMergePolicy.INSTANCE, new Lucene104Codec(), BEST_SPEED);
        assertFalse(policy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Lucene104Codec())));
        assertTrue(policy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_10_4_0, new Lucene104Codec())));
        assertTrue(policy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_9_12_2, new Lucene912Codec())));

        policy.setUpgradeInProgress(true, true);
        assertTrue(policy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_9_12_2, new Lucene912Codec())));
        assertFalse(policy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_10_4_0, new Lucene104Codec())));
    }

    public void testShouldUpgradeCodec() {
        var policy = new OpenSearchMergePolicy(NoMergePolicy.INSTANCE, new Lucene104Codec(), BEST_SPEED);
        assertTrue(policy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Lucene100Codec())));
        assertTrue(policy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Composite104Codec())));

        policy.setUpgradeInProgress(true, true);
        assertFalse(policy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Composite104Codec())));
    }

    public void testReopenedBestSpeedSegmentWithBestSpeedTarget() throws IOException {
        assertReopenedSegmentNeedsUpgrade(BEST_SPEED, BEST_SPEED, false);
    }

    public void testReopenedBestSpeedSegmentWithBestCompressionTarget() throws IOException {
        assertReopenedSegmentNeedsUpgrade(BEST_SPEED, BEST_COMPRESSION, true);
    }

    public void testReopenedBestCompressionSegmentWithBestSpeedTarget() throws IOException {
        assertReopenedSegmentNeedsUpgrade(BEST_COMPRESSION, BEST_SPEED, true);
    }

    public void testReopenedBestCompressionSegmentWithBestCompressionTarget() throws IOException {
        assertReopenedSegmentNeedsUpgrade(BEST_COMPRESSION, BEST_COMPRESSION, false);
    }

    public void testMissingCompressionMode() {
        var policy = new OpenSearchMergePolicy(NoMergePolicy.INSTANCE, codec(BEST_COMPRESSION), BEST_COMPRESSION);
        assertFalse(policy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Lucene104Codec())));
    }

    public void testUnknownTargetCompressionMode() throws IOException {
        try (Directory directory = newDirectory()) {
            writeSegment(directory, BEST_COMPRESSION);
            var policy = new OpenSearchMergePolicy(NoMergePolicy.INSTANCE, new Lucene104Codec(), null);
            assertFalse(policy.shouldUpgrade(SegmentInfos.readLatestCommit(directory).info(0)));
            assertTrue(policy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Composite104Codec())));
        }
    }

    public void testUpgradeFromBestSpeedToBestCompression() throws IOException {
        assertCompressionModeUpgrade(BEST_SPEED, BEST_COMPRESSION);
    }

    public void testUpgradeFromBestCompressionToBestSpeed() throws IOException {
        assertCompressionModeUpgrade(BEST_COMPRESSION, BEST_SPEED);
    }

    private void assertReopenedSegmentNeedsUpgrade(Mode sourceMode, Mode targetMode, boolean needsUpgrade) throws IOException {
        try (Directory directory = newDirectory()) {
            writeSegment(directory, sourceMode);
            SegmentCommitInfo segment = SegmentInfos.readLatestCommit(directory).info(0);
            assertEquals(sourceMode.name(), segment.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY));
            var policy = new OpenSearchMergePolicy(NoMergePolicy.INSTANCE, codec(targetMode), targetMode);
            assertEquals(needsUpgrade, policy.shouldUpgrade(segment));

            policy.setUpgradeInProgress(true, true);
            assertFalse(policy.shouldUpgrade(segment));
        }
    }

    private void assertCompressionModeUpgrade(Mode sourceMode, Mode targetMode) throws IOException {
        try (Directory directory = newDirectory()) {
            writeSegment(directory, sourceMode);
            var policy = new OpenSearchMergePolicy(NoMergePolicy.INSTANCE, codec(targetMode), targetMode);
            policy.setUpgradeInProgress(true, false);
            try (
                IndexWriter writer = new IndexWriter(
                    directory,
                    new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND)
                        .setCodec(codec(targetMode))
                        .setMergePolicy(policy)
                )
            ) {
                writer.forceMerge(1);
            }
            SegmentCommitInfo segment = SegmentInfos.readLatestCommit(directory).info(0);
            assertEquals(targetMode.name(), segment.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY));
            assertFalse(policy.shouldUpgrade(segment));
            // A subsequent upgrade request must not rewrite this already upgraded segment.
            policy.setUpgradeInProgress(true, false);
            try (
                IndexWriter writer = new IndexWriter(
                    directory,
                    new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND)
                        .setCodec(codec(targetMode))
                        .setMergePolicy(policy)
                )
            ) {
                writer.forceMerge(1);
            }
            assertEquals(segment.info.name, SegmentInfos.readLatestCommit(directory).info(0).info.name);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(1, reader.numDocs());
                assertEquals("stored field value", reader.storedFields().document(0).get("value"));
            }
        }
    }

    private void writeSegment(Directory directory, Mode mode) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setCodec(codec(mode)))) {
            Document document = new Document();
            document.add(new StoredField("value", "stored field value"));
            writer.addDocument(document);
        }
    }

    private Codec codec(Mode mode) {
        return new Lucene104Codec(Lucene104Codec.Mode.valueOf(mode.name()));
    }

    private SegmentCommitInfo createSegmentCommitInfo(Version version, Codec codec) {
        var id = new byte[16];
        var mockDir = Mockito.mock(Directory.class);
        var segmentInfo = new SegmentInfo(
            mockDir,
            version,
            version,
            "",
            Integer.MAX_VALUE,
            false,
            false,
            codec,
            Map.of(),
            id,
            Map.of(),
            null
        );
        return new SegmentCommitInfo(segmentInfo, 0, 0, 0, 0, 0, id);
    }
}
