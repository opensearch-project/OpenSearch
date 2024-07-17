/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.Collections;
import java.util.Set;

/**
 * Used when the derived field feature is disabled
 */
public class NoOpDerivedFieldResolver implements DerivedFieldResolver {

    @Override
    public Set<String> resolvePattern(String pattern) {
        return Collections.emptySet();
    }

    @Override
    public MappedFieldType resolve(String fieldName) {
        return null;
    }
}
