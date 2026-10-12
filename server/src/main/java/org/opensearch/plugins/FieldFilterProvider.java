/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Node-provided plugin component exposing the authoritative field-visibility filter.
 *
 * <p>The filter is the logical AND of every {@link MapperPlugin#getFieldFilter()} registered on the node. The outer function must be
 * evaluated with a concrete index name. Plugins that build query schemas or plans must apply the resulting field predicate before
 * resolving or validating user field references.
 *
 * <p>Mapper plugins may use the filter to implement authorization access controls such as field-level security. Consumers must treat
 * it as an access-control boundary and must not bypass it or reintroduce rejected fields.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldFilterProvider {

    private final Function<String, Predicate<String>> fieldFilter;

    /**
     * Creates a component exposing the given aggregated field filter.
     *
     * @param fieldFilter the node's aggregated field-visibility filter
     */
    public FieldFilterProvider(Function<String, Predicate<String>> fieldFilter) {
        this.fieldFilter = Objects.requireNonNull(fieldFilter);
    }

    /** Returns the node's aggregated field-visibility filter. */
    public Function<String, Predicate<String>> getFieldFilter() {
        return fieldFilter;
    }
}
