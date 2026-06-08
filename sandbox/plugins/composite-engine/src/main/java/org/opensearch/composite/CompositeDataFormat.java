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

    private final DataFormat primaryDataFormat;
    private final List<DataFormat> dataFormats;

    /**
     * Constructs a CompositeDataFormat with a designated primary format and a list of all constituent formats.
     *
     * @param primaryDataFormat the authoritative data format used for merge operations
     * @param dataFormats       all constituent data formats (including the primary)
     */
    public CompositeDataFormat(DataFormat primaryDataFormat, List<DataFormat> dataFormats) {
        this.primaryDataFormat = Objects.requireNonNull(primaryDataFormat, "primaryDataFormat must not be null");
        this.dataFormats = List.copyOf(Objects.requireNonNull(dataFormats, "dataFormats must not be null"));
    }

    /**
     * Constructs an empty CompositeDataFormat with no constituent formats.
     */
    public CompositeDataFormat() {
        this.primaryDataFormat = null;
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

    /**
     * Returns the primary data format used for merge operations.
     *
     * @return the primary data format
     */
    public DataFormat getPrimaryDataFormat() {
        return primaryDataFormat;
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
