/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * This interface defines the components involved in defining the scope of an application in OpenSearch
 *
 * @opensearch.experimental
 */
public interface Scope extends Writeable {

    String UNKNOWN_SCOPE_MESSAGE = "Failed to find scope: ";

    ScopeEnums.ScopeNamespace getNamespace();

    ScopeEnums.ScopeArea getArea();

    String getAction();

    default String asPermissionString() {
        return getNamespace() + "." + getArea().toString() + "." + getAction();
    }

    static Scope parseScopeFromString(String scopeAsString) {

        String[] parts = scopeAsString.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid scope format: " + scopeAsString);
        }
        ScopeEnums.ScopeNamespace scopeNamespace = ScopeEnums.ScopeNamespace.fromString(parts[0]);
        ScopeEnums.ScopeArea scopeArea = ScopeEnums.ScopeArea.fromString(parts[1]);
        String action = parts[2];

        switch (scopeNamespace) {
            case ACTION:
                for (ActionScope actionScope : ActionScope.values()) {
                    if (actionScope.getArea().equals(scopeArea) && actionScope.action.equals(action)) {
                        return actionScope;
                    }
                }
                throw new RuntimeException(UNKNOWN_SCOPE_MESSAGE + scopeAsString);
            case APPLICATION:
                for (ApplicationScope applicationScope : ApplicationScope.values()) {
                    if (applicationScope.getArea().equals(scopeArea) && applicationScope.action.equals(action)) {
                        return applicationScope;
                    }
                }
                throw new RuntimeException(UNKNOWN_SCOPE_MESSAGE + scopeAsString);
            case EXTENSION_POINT:
                for (ExtensionPointScope extensionPointScope : ExtensionPointScope.values()) {
                    if (extensionPointScope.getArea().equals(scopeArea) && extensionPointScope.action.equals(action)) {
                        return extensionPointScope;
                    }
                }
                throw new RuntimeException(UNKNOWN_SCOPE_MESSAGE + scopeAsString);
            default:
                throw new RuntimeException(UNKNOWN_SCOPE_MESSAGE + scopeAsString);
        }
    }

    /**
     * Create a new scope from a stream input
     *
     * @throws IOException if an I/O exception occurred reading the scope information
     */
    public static Scope readStream(StreamInput in) throws IOException {
        String scopeAsString = in.readString();
        return Scope.parseScopeFromString(scopeAsString);
    }

    /**
     * Write a scope out to a stream output
     * @param out The output
     * @throws IOException if an I/O exception occurred writing the scope information
     */
    public default void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.asPermissionString());
    }
}
