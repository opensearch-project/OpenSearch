/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A composite {@link DataFormat} that wraps multiple per-format {@link DataFormat} instances.
 * Each constituent format retains its own {@link FieldTypeCapabilities} — field routing is
 * handled per-format by {@link CompositeDocumentInput}, not by this class.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDataFormat extends DataFormat {

    private final List<DataFormat> dataFormats;

    /**
     * Constructs a CompositeDataFormat from the given list of data formats.
     *
     * @param dataFormats the constituent data formats
     */
    public CompositeDataFormat(List<DataFormat> dataFormats) {
        this.dataFormats = List.copyOf(Objects.requireNonNull(dataFormats, "dataFormats must not be null"));
    }

    /**
     * Constructs an empty CompositeDataFormat with no constituent formats.
     */
    public CompositeDataFormat() {
        this.dataFormats = List.of();
    }

    /**
     * Returns the list of constituent data formats.
     *
     * @return the data formats
     */
    public List<DataFormat> getDataFormats() {
        return dataFormats;
    }

    @Override
    public String name() {
        return "composite";
    }

    @Override
    public long priority() {
        // In case some other format can independently support,
        // the composite format should have the lowest priority
        return Long.MIN_VALUE;
    }

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        // Union of all constituent formats' supported fields
        // TODO:: Post the changes done in mappings, we will relook this
        if (dataFormats.isEmpty()) {
            return Set.of();
        }
        return dataFormats.get(0).supportedFields();
    }

    @Override
    public String toString() {
        return "CompositeDataFormat{dataFormats=" + dataFormats + '}';
    }
}
