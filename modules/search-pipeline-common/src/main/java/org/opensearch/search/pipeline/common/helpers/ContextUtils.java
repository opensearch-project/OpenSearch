/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common.helpers;

/**
 * Helpers for working with request-scoped context.
 */
public final class ContextUtils {
    private ContextUtils() {}

    /**
     * Parameter that can be passed to a stateful processor to avoid collisions between contextual variables by
     * prefixing them with distinct qualifiers.
     */
    public static final String CONTEXT_PREFIX_PARAMETER = "context_prefix";

    /**
     * Replaces a "global" variable name with one scoped to a given context prefix (unless prefix is null or empty).
     * @param contextPrefix the prefix qualifier for the variable
     * @param variableName the generic "global" form of the context variable
     * @return the variableName prefixed with contextPrefix followed by ".", or just variableName if contextPrefix is null or empty
     */
    public static String applyContextPrefix(String contextPrefix, String variableName) {
        String contextVariable;
        if (contextPrefix != null && contextPrefix.isEmpty() == false) {
            contextVariable = contextPrefix + "." + variableName;
        } else {
            contextVariable = variableName;
        }
        return contextVariable;
    }
}
