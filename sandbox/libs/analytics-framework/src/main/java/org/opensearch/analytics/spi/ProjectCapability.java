/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Declares a backend's ability to evaluate projection expressions, scoped to data formats.
 * Two variants: standard scalar functions and opaque backend-specific operations.
 *
 * @opensearch.internal
 */
public sealed interface ProjectCapability {

    /** Standard scalar function (CAST, PLUS, UPPER, etc.) on a field type in given formats. */
    record Scalar(ScalarFunction function, FieldType fieldType, Set<String> formats) implements ProjectCapability {
        public Scalar {
            formats = Set.copyOf(formats);
        }
    }

    /** Opaque backend-specific operation (painless, highlight, suggest, etc.) in given formats. */
    record Opaque(String name, Set<String> formats) implements ProjectCapability {
        public Opaque {
            formats = Set.copyOf(formats);
        }
    }
}
