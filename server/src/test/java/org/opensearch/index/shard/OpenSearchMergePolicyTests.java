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
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormatComparator;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.opensearch.index.codec.composite.composite101.Composite101Codec;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import org.mockito.Mockito;

import static org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat.Mode.BEST_COMPRESSION;
import static org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat.Mode.BEST_SPEED;

/**
 * Very simple checks for {@link OpenSearchMergePolicy}
 */
public class OpenSearchMergePolicyTests extends OpenSearchTestCase {
    public void testShouldUpgrade() {
        var mockMergePolicy = Mockito.mock(MergePolicy.class);
        var openSearchMergePolicy = new OpenSearchMergePolicy(mockMergePolicy, new Lucene101Codec());
        assertFalse(openSearchMergePolicy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Lucene101Codec())));
        assertTrue(openSearchMergePolicy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_10_2_0, new Lucene100Codec())));
        assertTrue(
            openSearchMergePolicy.shouldUpgrade(
                createSegmentCommitInfo(Version.LATEST, new Lucene101Codec(Lucene101Codec.Mode.BEST_COMPRESSION))
            )
        );
        assertTrue(openSearchMergePolicy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_9_12_2, new Lucene912Codec())));
        assertTrue(openSearchMergePolicy.shouldUpgrade(createSegmentCommitInfo(Version.LATEST, new Composite101Codec())));

        openSearchMergePolicy.setUpgradeInProgress(true, true);
        assertTrue(openSearchMergePolicy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_9_12_2, new Lucene912Codec())));
        assertFalse(openSearchMergePolicy.shouldUpgrade(createSegmentCommitInfo(Version.LUCENE_10_0_0, new Lucene100Codec())));
    }

    /**
     * Very simple equal checks for {@link Lucene90StoredFieldsFormatComparator}
     */
    public void testLucene90StoredFieldsFormatComparatorEqual() {
        assertTrue(Lucene90StoredFieldsFormatComparator.equal(new Lucene90StoredFieldsFormat(), new Lucene90StoredFieldsFormat()));
        assertTrue(
            Lucene90StoredFieldsFormatComparator.equal(new Lucene90StoredFieldsFormat(), new Lucene90StoredFieldsFormat(BEST_SPEED))
        );
        assertTrue(
            Lucene90StoredFieldsFormatComparator.equal(
                new Lucene90StoredFieldsFormat(BEST_SPEED),
                new Lucene90StoredFieldsFormat(BEST_SPEED)
            )
        );
        assertTrue(
            Lucene90StoredFieldsFormatComparator.equal(
                new Lucene90StoredFieldsFormat(BEST_COMPRESSION),
                new Lucene90StoredFieldsFormat(BEST_COMPRESSION)
            )
        );
        assertFalse(
            Lucene90StoredFieldsFormatComparator.equal(new Lucene90StoredFieldsFormat(), new Lucene90StoredFieldsFormat(BEST_COMPRESSION))
        );
        assertFalse(
            Lucene90StoredFieldsFormatComparator.equal(
                new Lucene90StoredFieldsFormat(BEST_SPEED),
                new Lucene90StoredFieldsFormat(BEST_COMPRESSION)
            )
        );
        assertFalse(
            Lucene90StoredFieldsFormatComparator.equal(
                new Lucene90StoredFieldsFormat(BEST_COMPRESSION),
                new Lucene90StoredFieldsFormat(BEST_SPEED)
            )
        );
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
