/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.transport;

import org.opensearch.action.ActionType;

public class AllocationServiceAction  extends ActionType<RerouteActionResponse> {
    /**
     * The name to look up this action with
     */
    public static final String NAME = "allocationservice/remote";
    /**
     * The singleton instance of this class
     */
    public static final AllocationServiceAction INSTANCE = new AllocationServiceAction();

    private AllocationServiceAction() {
        super(NAME, RerouteActionResponse::new);
    }

}
