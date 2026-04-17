/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetResponse;

public class GetSavedObjectAction extends ActionType<GetResponse> {

    public static final GetSavedObjectAction INSTANCE = new GetSavedObjectAction();
    public static final String NAME = "osd:saved_object/get";

    private GetSavedObjectAction() {
        super(NAME, in -> {
            return new GetResponse(new org.opensearch.index.get.GetResult(in));
        });
    }
}
