/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.CheckedFunction;
import org.opensearch.index.codec.CriteriaBasedCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper merge policy which is used for context aware enabled indices. This merge policy merges segments that belongs
 * to same bucket.
 *
 */
public class CriteriaBasedMergePolicy extends FilterMergePolicy {

    protected final MergePolicy in;

    public CriteriaBasedMergePolicy(MergePolicy in) {
        super(in);
        this.in = in;
    }

    private MergeSpecification findMergesInternal(
        SegmentInfos segmentInfos,
        MergeContext mergeContext,
        CheckedFunction<SegmentInfos, MergeSpecification, IOException> mergeFinderFunction
    ) throws IOException {

        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
        final Map<String, List<SegmentCommitInfo>> commitInfos = new HashMap<>();

        for (SegmentCommitInfo si : segmentInfos) {
            if (merging.contains(si)) {
                continue;
            }
            final String dwptGroupNumber = si.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME);
            commitInfos.computeIfAbsent(dwptGroupNumber, k -> new ArrayList<>()).add(si);
        }

        MergeSpecification spec = null;
        for (Map.Entry<String, List<SegmentCommitInfo>> entry : commitInfos.entrySet()) {
            List<SegmentCommitInfo> segments = entry.getValue();
            if (segments.size() > 1) {
                final SegmentInfos newSIS = new SegmentInfos(segmentInfos.getIndexCreatedVersionMajor());
                segments.forEach(newSIS::add);

                final MergeSpecification delegateSpec = mergeFinderFunction.apply(newSIS);
                if (delegateSpec != null) {
                    if (spec == null) {
                        spec = new MergeSpecification();
                    }
                    spec.merges.addAll(delegateSpec.merges);
                }
            }
        }

        return spec;
    }

    /**
     * Merges the segments belonging to same group
     *
     * @param mergeTrigger the event that triggered the merge
     * @param infos the total set of segments in the index
     * @param mergeContext the IndexWriter to find the merges on
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        return findMergesInternal(infos, mergeContext, newSIS -> in.findMerges(mergeTrigger, newSIS, mergeContext));
    }

    /**
     * Force merges segments belonging to same group.
     *
     * @param segmentInfos the total set of segments in the index
     * @param maxSegmentCount requested maximum number of segments in the index
     * @param segmentsToMerge contains the specific SegmentInfo instances that must be merged away.
     * @param mergeContext the MergeContext to find the merges on
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos segmentInfos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext
    ) throws IOException {
        return findMergesInternal(
            segmentInfos,
            mergeContext,
            newSIS -> in.findForcedMerges(newSIS, maxSegmentCount, segmentsToMerge, mergeContext)
        );
    }

    /**
     * Merges segment belonging to same group to expunge deletes.
     *
     * @param segmentInfos the total set of segments in the index
     * @param mergeContext the MergeContext to find the merges on
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        return findMergesInternal(segmentInfos, mergeContext, newSIS -> in.findForcedDeletesMerges(newSIS, mergeContext));
    }

    /**
     * Identifies merges that we want to execute (synchronously) on commit
     *
     * @param mergeTrigger the event that triggered the merge (COMMIT or GET_READER).
     * @param segmentInfos the total set of segments in the index (while preparing the commit)
     * @param mergeContext the MergeContext to find the merges on.
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findFullFlushMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
        return findMergesInternal(segmentInfos, mergeContext, newSIS -> in.findFullFlushMerges(mergeTrigger, newSIS, mergeContext));
    }
}
