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
 * Action for creating or updating a saved object.
 */
public class WriteSavedObjectAction extends ActionType<SavedObjectResponse> {

    public static final WriteSavedObjectAction INSTANCE = new WriteSavedObjectAction();
    public static final String NAME = "osd:saved_object/write";

    private WriteSavedObjectAction() {
        super(NAME, SavedObjectResponse::new);
    }
}
