/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.index.mapper.MappedFieldType;

/**
 * Resolves a field's mapping for aggregation rendering, decoupled from any single index. On a
 * single-index request this resolves against that index's mapping; on a multi-index request it
 * resolves first-non-null across the request's indices in resolution order, which is safe because
 * the schema-equivalence gate has already proven the defining indices agree on how each referenced
 * bucket field renders. A {@code null} lookup means no mapping is available at all (conversion-only
 * use); a {@code null} result means no resolved index defines the field.
 */
@FunctionalInterface
public interface FieldTypeLookup {

    /**
     * @param field the full (dotted) field name
     * @return the field's mapping, or {@code null} when no resolved index defines it
     */
    MappedFieldType fieldType(String field);
}
