/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.coord.Segment;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.List;

public class OneMerge {
    private final List<Segment> segmentsToMerge;

    public OneMerge(List<Segment> segmentsToMerge) {
        this.segmentsToMerge = segmentsToMerge;
    }

    public List<Segment> getSegmentsToMerge() {
        return segmentsToMerge;
    }

    public String toString() {
        return "Merge [SegmentsToMerge=" + segmentsToMerge + "] ";
    }
}
