/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.index.BaseMergePolicyTestCase;
import org.apache.lucene.util.Version;
import org.opensearch.index.codec.CriteriaBasedCodec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CriteriaBasedMergePolicyTests extends OpenSearchTestCase {

    public void testFindMerges_EmptySegments() throws IOException {
        CriteriaBasedMergePolicy mergePolicy = new CriteriaBasedMergePolicy(new TieredMergePolicy());
        SegmentInfos infos = createSegmentInfos();
        MergePolicy.MergeSpecification result = mergePolicy.findMerges(
            MergeTrigger.SEGMENT_FLUSH,
            infos,
            new BaseMergePolicyTestCase.MockMergeContext(SegmentCommitInfo::getDelCount)
        );

        assertNull(result);
    }

    public void testFindMerges_AllSegmentsMerging() throws IOException {
        CriteriaBasedMergePolicy mergePolicy = new CriteriaBasedMergePolicy(new TieredMergePolicy());
        SegmentInfos infos = createSegmentInfos();
        SegmentCommitInfo sci1 = createSegmentCommitInfo("_1", "bucket1");
        SegmentCommitInfo sci2 = createSegmentCommitInfo("_2", "bucket1");
        infos.add(sci1);
        infos.add(sci2);

        try {
            Set<SegmentCommitInfo> mergingSegments = new HashSet<>(Arrays.asList(sci1, sci2));
            BaseMergePolicyTestCase.MockMergeContext mergeContext = new BaseMergePolicyTestCase.MockMergeContext(
                SegmentCommitInfo::getDelCount
            );
            mergeContext.setMergingSegments(mergingSegments);

            MergePolicy.MergeSpecification result = mergePolicy.findMerges(MergeTrigger.FULL_FLUSH, infos, mergeContext);

            assertNull(result);
        } finally {
            sci1.info.dir.close();
            sci2.info.dir.close();
        }
    }

    public void testFindMerges_MultipleSegmentsInSingleGroup() throws IOException {
        CriteriaBasedMergePolicy mergePolicy = new CriteriaBasedMergePolicy(new TieredMergePolicy());
        SegmentInfos infos = createSegmentInfos();
        SegmentCommitInfo sci1 = createSegmentCommitInfo("_1", "bucket1");
        SegmentCommitInfo sci2 = createSegmentCommitInfo("_2", "bucket1");
        infos.add(sci1);
        infos.add(sci2);
        BaseMergePolicyTestCase.MockMergeContext mergeContext = new BaseMergePolicyTestCase.MockMergeContext(
            SegmentCommitInfo::getDelCount
        );
        mergeContext.setMergingSegments(Collections.emptySet());
        MergePolicy.MergeSpecification result = mergePolicy.findMerges(MergeTrigger.FULL_FLUSH, infos, mergeContext);

        assertNotNull(result);
        assertEquals(1, result.merges.size());
    }

    private SegmentInfos createSegmentInfos() {
        return new SegmentInfos(Version.LATEST.major);
    }

    private SegmentCommitInfo createSegmentCommitInfo(String segmentName, String bucketName) throws IOException {
        Directory directory = mock(FSDirectory.class);
        when(directory.fileLength(any())).thenReturn(5368709120L);
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_10_1_0,
            segmentName,
            5,
            false,
            false,
            new Lucene103Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        segmentInfo.putAttribute(CriteriaBasedCodec.BUCKET_NAME, bucketName);
        segmentInfo.setFiles(List.of(segmentName + ".cfe"));
        return new SegmentCommitInfo(segmentInfo, 5, 10, 1, 0, 0, null);
    }
}
