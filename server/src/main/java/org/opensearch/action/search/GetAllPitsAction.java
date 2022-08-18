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
 * Action type for listing all PIT reader contexts
 */
public class GetAllPitsAction extends ActionType<GetAllPitNodesResponse> {
    public static final GetAllPitsAction INSTANCE = new GetAllPitsAction();
    public static final String NAME = "indices:data/read/point_in_time/readall";

    private GetAllPitsAction() {
        super(NAME, GetAllPitNodesResponse::new);
    }
}
