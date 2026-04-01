/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Declares that a backend can evaluate a specific {@link FilterOperator}
 * on a specific {@link FieldTypeFamily}.
 *
 * <p>Example: {@code FilterCapability.of(EQUALS, KEYWORD)} means the backend
 * can evaluate equality predicates on keyword fields.
 *
 * @opensearch.internal
 */
public record FilterCapability(FilterOperator operator, FieldTypeFamily fieldTypeFamily) {

    public static FilterCapability of(FilterOperator operator, FieldTypeFamily fieldTypeFamily) {
        return new FilterCapability(operator, fieldTypeFamily);
    }
}
