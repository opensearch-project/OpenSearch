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
 * Action for deleting a saved object.
 */
public class DeleteSavedObjectAction extends ActionType<SavedObjectResponse> {

    public static final DeleteSavedObjectAction INSTANCE = new DeleteSavedObjectAction();
    public static final String NAME = "osd:saved_object/delete";

    private DeleteSavedObjectAction() {
        super(NAME, SavedObjectResponse::new);
    }
}
