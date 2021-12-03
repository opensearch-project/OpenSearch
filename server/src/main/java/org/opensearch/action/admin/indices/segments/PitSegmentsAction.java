/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.ActionType;

public class PitSegmentsAction extends ActionType<IndicesSegmentResponse> {

    public static final PitSegmentsAction INSTANCE = new PitSegmentsAction();
    public static final String NAME = "indices:monitor/pitsegment";

    private PitSegmentsAction() {
        super(NAME, IndicesSegmentResponse::new);
    }
}
