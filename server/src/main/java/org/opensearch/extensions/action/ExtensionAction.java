/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionType;

/**
 * An {@link ActionType} to be used in extension action transport handlers.
 *
 * @opensearch.internal
 */
public class ExtensionAction extends ActionType<ExtensionActionResponse> {

    private final String uniqueId;

    /**
     * Create an instance of this action to register in the dynamic actions map.
     *
     * @param uniqueId The uniqueId of the extension which will run this action.
     * @param name The fully qualified class name of the extension's action to execute.
     */
    ExtensionAction(String uniqueId, String name) {
        super(name, ExtensionActionResponse::new);
        this.uniqueId = uniqueId;
    }

    /**
     * Gets the uniqueId of the extension which will run this action.
     *
     * @return the uniqueId
     */
    public String getUniqueId() {
        return this.uniqueId;
    }
}
