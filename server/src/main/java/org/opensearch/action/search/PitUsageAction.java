/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

/**
 * Action for retrieving usage information for PITs
 */
public class PitUsageAction extends ActionType<PitUsageResponse> {

    public static final PitUsageAction INSTANCE = new PitUsageAction();
    public static final String NAME = "indices:data/read/point_in_time/usage";

    private PitUsageAction() {
        super(NAME, PitUsageResponse::new);
    }
}
