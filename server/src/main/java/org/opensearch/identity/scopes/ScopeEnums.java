/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.util.Arrays;
import java.util.Locale;

/**
 * This class is used as a holder for the different enums associated with Scopes.
 */
public class ScopeEnums {

    /**
     * This enum tracks the constants for the different ScopeAreas
     */
    public enum ScopeArea {
        APPLICATION,
        ACTION_PLUGIN,
        CLUSTER,
        EXTENSION_POINT,
        IMPERSONATE,
        INDEX,
        SUPER_USER_ACCESS,
        SYSTEM_INDEX,
        ALL;

        public static ScopeArea fromString(String value) {
            if (Arrays.stream(ScopeArea.values()).map(ScopeArea::toString).anyMatch(val -> val.equals(value))) {
                return ScopeArea.valueOf(value.toUpperCase(Locale.ROOT));
            }
            throw new RuntimeException("Unknown ScopeArea: " + value);
        }
    }

    /**
     * This enum tracks the constants for the different ScopeNamespaces
     */
    public enum ScopeNamespace {
        ACTION,
        APPLICATION,
        EXTENSION_POINT;

        public static ScopeNamespace fromString(String value) {
            if (Arrays.stream(ScopeNamespace.values()).map(ScopeNamespace::toString).anyMatch(val -> val.equals(value))) {
                return ScopeNamespace.valueOf(value.toUpperCase(Locale.ROOT));
            }
            throw new RuntimeException("Unknown ScopeNamespace: " + value);
        }
    }
}
