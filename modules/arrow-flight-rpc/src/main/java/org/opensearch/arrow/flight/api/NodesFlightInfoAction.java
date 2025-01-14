/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.action.ActionType;

public class NodesFlightInfoAction extends ActionType<NodesFlightInfoResponse> {
    public static final NodesFlightInfoAction INSTANCE = new NodesFlightInfoAction();
    public static final String NAME = "cluster:admin/flight/info";

    NodesFlightInfoAction() {
        super(NAME, NodesFlightInfoResponse::new);
    }
}
