/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Wire-format contract a {@link FragmentConvertor} declares for the bytes it produces from
 * {@link FragmentConvertor#convertFragment}. The framework consults this when a parent stage
 * needs to derive a partition schema for the child stage's output — different formats need
 * different derivation strategies.
 *
 * <p>The framework does not know any specific format; backends pick a value based on whether
 * their bytes carry a self-describing schema the reducer can decode generically.
 *
 * @opensearch.internal
 */
public enum WireFormat {
    /**
     * Bytes carry enough type information for the framework to derive the child stage's
     * partition schema without backend help (e.g. Substrait plans whose Read rel exposes a
     * named struct). The reducer decodes the bytes directly.
     */
    SELF_DESCRIBING,

    /**
     * Bytes are opaque to the framework — the reducer cannot derive a partition schema from
     * them without backend help. Backends declaring {@code OPAQUE} MUST also override
     * {@link FragmentConvertor#convertSchemaOnlyRead} so the framework can emit a separate
     * schema stub at the partition boundary.
     */
    OPAQUE
}
