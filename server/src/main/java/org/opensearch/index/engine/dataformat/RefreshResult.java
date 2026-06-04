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

/**
 * Result of a refresh operation containing the refreshed segments.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record RefreshResult(List<Segment> refreshedSegments) {
}
