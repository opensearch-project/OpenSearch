/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.api.action;

import org.opensearch.action.ActionType;

public class APIAction extends ActionType<APIResponse> {

    public static final String NAME = "api";
    public static final APIAction INSTANCE = new APIAction();

    public APIAction() {
        super(NAME, APIResponse::new);
    }
}
