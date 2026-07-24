/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.Set;

/**
 * A boolean mapping parameter contributed by a data-format plugin to the {@code keyword} and {@code text}
 * field mappers. When {@link #disablesIndexing()} is set, a resolved value of {@code true} disables Lucene
 * indexing for the field.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PluginMappingParameter {

    private final String name;
    private final boolean defaultValue;
    private final boolean updateable;
    private final Set<String> applicableContentTypes;
    private final boolean disablesIndexing;

    public PluginMappingParameter(
        String name,
        boolean defaultValue,
        boolean updateable,
        Set<String> applicableContentTypes,
        boolean disablesIndexing
    ) {
        this.name = Objects.requireNonNull(name, "name");
        this.defaultValue = defaultValue;
        this.updateable = updateable;
        this.applicableContentTypes = Set.copyOf(Objects.requireNonNull(applicableContentTypes, "applicableContentTypes"));
        this.disablesIndexing = disablesIndexing;
    }

    public String name() {
        return name;
    }

    public boolean defaultValue() {
        return defaultValue;
    }

    public boolean updateable() {
        return updateable;
    }

    public Set<String> applicableContentTypes() {
        return applicableContentTypes;
    }

    public boolean disablesIndexing() {
        return disablesIndexing;
    }

    /** Returns {@code true} if this parameter applies to the given mapper content type. */
    public boolean appliesTo(String contentType) {
        return applicableContentTypes.contains(contentType);
    }
}
