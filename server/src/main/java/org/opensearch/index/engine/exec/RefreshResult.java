/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.coord.Segment;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.ArrayList;
import java.util.List;

public class RefreshResult {

    private List<Segment> refreshedSegments;

    public RefreshResult() {
        this.refreshedSegments = new ArrayList<>();
    }

    public List<Segment> getRefreshedSegments() {
        return refreshedSegments;
    }

    public void setRefreshedSegments(List<Segment> refreshedSegments) {
        this.refreshedSegments = refreshedSegments;
    }
}
