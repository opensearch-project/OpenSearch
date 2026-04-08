/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.ActionType;

/**
 * Action for getting a single saved object by type and id.
 */
public class GetSavedObjectAction extends ActionType<SavedObjectResponse> {

    public static final GetSavedObjectAction INSTANCE = new GetSavedObjectAction();
    public static final String NAME = "osd:saved_object/get";

    private GetSavedObjectAction() {
        super(NAME, SavedObjectResponse::new);
    }
}
