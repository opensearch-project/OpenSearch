/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

/**
 * Provide contextual profile breakdowns which are associated with freestyle context. Used when concurrent
 * search over segments is activated and each collector needs own non-shareable profile breakdown instance.
 */
public abstract class ContextualProfileBreakdown<T extends Enum<T>> extends AbstractProfileBreakdown<T> {
    public ContextualProfileBreakdown(Class<T> clazz) {
        super(clazz);
    }

    /**
     * Return (or create) contextual profile breakdown instance
     * @param context freestyle context
     * @return contextual profile breakdown instance
     */
    public abstract AbstractProfileBreakdown<T> context(Object context);
}
