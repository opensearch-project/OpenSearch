/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a data format for storing and managing index data, with declared capabilities.
 * Each data format (e.g., Lucene, Parquet) declares what storage and query capabilities it supports.
 * <p>
 * Equality is based on the format {@link #name()} — there should be one {@code DataFormat} instance
 * per unique name. This allows {@code DataFormat} to be used safely as a {@link java.util.Map} key.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class DataFormat {
    /**
     * Returns the unique name of this data format.
     *
     * @return the data format name
     */
    public abstract String name();

    /**
     * Returns the priority of this data format. Higher priority formats are preferred
     * when multiple formats can handle the same field type.
     *
     * @return the priority value
     */
    public abstract long priority();

    /**
     * Returns the set of field type capabilities supported by this data format.
     *
     * @return the supported field type capabilities
     */
    public abstract Set<FieldTypeCapabilities> supportedFields();

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DataFormat == false) return false;
        return Objects.equals(name(), ((DataFormat) o).name());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(name());
    }
}
