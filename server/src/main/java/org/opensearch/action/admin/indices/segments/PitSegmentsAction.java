/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segments;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.identity.Scope;

/**
 * Action for retrieving segment information for PITs
 */
public class PitSegmentsAction extends ActionType<IndicesSegmentResponse> {

    public static final PitSegmentsAction INSTANCE = new PitSegmentsAction();
    public static final String NAME = "indices:monitor/point_in_time/segments";

    private PitSegmentsAction() {
        super(NAME, IndicesSegmentResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Index_ALL);
    }
}
