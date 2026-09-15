/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;

import java.util.List;
import java.util.Set;

/**
 * Result of a refresh operation: the refreshed segments plus any generations that were fully deleted
 * during the refresh cycle (dropped from the catalog so it stays consistent with the reader).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record RefreshResult(List<Segment> refreshedSegments, Set<Long> droppedGenerations) {
    public RefreshResult(List<Segment> refreshedSegments) {
        this(refreshedSegments, Set.of());
    }
}
