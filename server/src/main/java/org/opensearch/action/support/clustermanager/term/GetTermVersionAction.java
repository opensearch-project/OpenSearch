/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.opensearch.action.ActionType;

/**
 * Transport action for fetching cluster term and version
 *
 * @opensearch.internal
 */
public class GetTermVersionAction extends ActionType<GetTermVersionResponse> {

    public static final GetTermVersionAction INSTANCE = new GetTermVersionAction();
    public static final String NAME = "internal:monitor/term";

    private GetTermVersionAction() {
        super(NAME, GetTermVersionResponse::new);
    }
}
