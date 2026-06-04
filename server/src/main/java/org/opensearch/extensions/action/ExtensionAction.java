/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionType;

import java.util.Objects;

/**
 * An {@link ActionType} to be used in extension action transport handling.
 *
 * @opensearch.internal
 */
public class ExtensionAction extends ActionType<RemoteExtensionActionResponse> {

    private final String uniqueId;

    /**
     * Create an instance of this action to register in the dynamic actions map.
     *
     * @param uniqueId The uniqueId of the extension which will run this action.
     * @param name The fully qualified class name of the extension's action to execute.
     */
    public ExtensionAction(String uniqueId, String name) {
        super(name, RemoteExtensionActionResponse::new);
        this.uniqueId = uniqueId;
    }

    /**
     * Gets the uniqueId of the extension which will run this action.
     *
     * @return the uniqueId
     */
    public String uniqueId() {
        return this.uniqueId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(uniqueId);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        ExtensionAction other = (ExtensionAction) obj;
        return Objects.equals(uniqueId, other.uniqueId);
    }
}
