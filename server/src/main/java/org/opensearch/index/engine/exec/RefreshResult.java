/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.ArrayList;
import java.util.List;

public class RefreshResult {

    private List<CatalogSnapshot.Segment> refreshedSegments;

    public RefreshResult() {
        this.refreshedSegments = new ArrayList<>();
    }

    public List<CatalogSnapshot.Segment> getRefreshedSegments() {
        return refreshedSegments;
    }

    public void setRefreshedSegments(List<CatalogSnapshot.Segment> refreshedSegments) {
        this.refreshedSegments = refreshedSegments;
    }
}
