/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Provides merge candidate selection and merging-segment tracking for
 * data-format-aware merge policies.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface MergePolicyProvider {

    /**
     * Finds merge candidates from the given segments.
     *
     * @param segments the current list of segments
     * @return a list of segment groups, each group representing one merge operation
     * @throws IOException if an I/O error occurs during candidate selection
     */
    List<List<Segment>> findMergeCandidates(List<Segment> segments) throws IOException;

    /**
     * Finds force-merge candidates targeting the specified maximum segment count.
     *
     * @param segments        the current list of segments
     * @param maxSegmentCount the target maximum number of segments after merging
     * @return a list of segment groups, each group representing one merge operation
     * @throws IOException if an I/O error occurs during candidate selection
     */
    List<List<Segment>> findForceMergeCandidates(List<Segment> segments, int maxSegmentCount) throws IOException;

    /**
     * Registers segments as currently merging.
     *
     * @param segments the segments being merged
     */
    void addMergingSegment(Collection<Segment> segments);

    /**
     * Removes segments from the currently-merging set.
     *
     * @param segments the segments to remove
     */
    void removeMergingSegment(Collection<Segment> segments);
}
