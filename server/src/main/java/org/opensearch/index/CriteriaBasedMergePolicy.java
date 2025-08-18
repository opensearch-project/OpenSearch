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

    /**
     * Merges the segments belonging to same group.
     *
     * @param mergeTrigger the event that triggered the merge
     * @param infos the total set of segments in the index
     * @param mergeContext the IndexWriter to find the merges on
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
        MergeSpecification spec = null;
        final Map<String, List<SegmentCommitInfo>> commitInfos = new HashMap<>();
        for (SegmentCommitInfo si : infos) {
            if (merging.contains(si)) {
                continue;
            }

            final String dwptGroupNumber = si.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME);
            commitInfos.computeIfAbsent(dwptGroupNumber, k -> new ArrayList<>()).add(si);
        }

        for (String dwptGroupNumber : commitInfos.keySet()) {
            if (commitInfos.get(dwptGroupNumber).size() > 1) {
                final SegmentInfos newSIS = new SegmentInfos(infos.getIndexCreatedVersionMajor());
                for (SegmentCommitInfo info : commitInfos.get(dwptGroupNumber)) {
                    newSIS.add(info);
                }

                final MergeSpecification tieredMergePolicySpec = in.findMerges(mergeTrigger, newSIS, mergeContext);
                if (tieredMergePolicySpec != null) {
                    if (spec == null) {
                        spec = new MergeSpecification();
                    }

                    spec.merges.addAll(tieredMergePolicySpec.merges);
                }
            }
        }

        return spec;
    }
}
