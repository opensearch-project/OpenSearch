/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.PublicApi;

import java.util.Set;

/**
 * DerivedFieldResolver is used as a lookup to resolve derived fields from their name.
 * It is created per search request and needs to be set at {@link org.opensearch.index.query.QueryShardContext#setDerivedFieldResolver(DerivedFieldResolver)}
 * for derived fields resolution.
 */
@PublicApi(since = "2.15.0")
public interface DerivedFieldResolver {
    /**
     * Resolves all derived fields matching a given pattern. It includes derived fields defined both in search requests
     * and index mapping.
     * @param pattern regex pattern
     * @return all derived fields matching the pattern
     */
    Set<String> resolvePattern(String pattern);

    /**
     * Resolves the MappedFieldType associated with a derived field
     * @param fieldName field name to lookup
     * @return mapped field type
     */
    MappedFieldType resolve(String fieldName);

}
