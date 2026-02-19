/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of a refresh operation containing the refreshed segments.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RefreshResult {

    private List<org.opensearch.index.engine.exec.coord.Segment> refreshedSegments;

    /**
     * Constructs a new refresh result with an empty list of segments.
     */
    public RefreshResult() {
        this.refreshedSegments = new ArrayList<>();
    }

    /**
     * Gets the list of refreshed segments.
     *
     * @return the refreshed segments list
     */
    public List<org.opensearch.index.engine.exec.coord.Segment> getRefreshedSegments() {
        return refreshedSegments;
    }

    /**
     * Sets the refreshed segments.
     *
     * @param refreshedSegments the list of refreshed segments
     */
    public void setRefreshedSegments(List<org.opensearch.index.engine.exec.coord.Segment> refreshedSegments) {
        this.refreshedSegments = refreshedSegments;
    }
}
