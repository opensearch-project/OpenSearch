/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.metadata.compress.CompressedData;

import java.io.IOException;
import java.util.Objects;

/**
 * Pure data holder class for mapping metadata without dependencies on OpenSearch server packages.
 * This class follows the composition pattern and contains only data fields with getters.
 * <p>
 * MappingMetadataModel stores all essential properties of a mapping.
 */
@ExperimentalApi
public final class MappingMetadataModel implements Writeable {

    private final String type;
    private final CompressedData source;
    private final boolean routingRequired;

    /**
     * Creates a new MappingMetadataModel.
     *
     * @param type the mapping type name (must not be null)
     * @param source the compressed mapping source (must not be null)
     * @param routingRequired whether routing is required
     * @throws NullPointerException if type or source is null
     */
    public MappingMetadataModel(String type, CompressedData source, boolean routingRequired) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.routingRequired = routingRequired;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public MappingMetadataModel(StreamInput in) throws IOException {
        this.type = in.readString();
        this.source = new CompressedData(in);
        this.routingRequired = in.readBoolean();
    }

    /**
     * Returns the mapping type name.
     *
     * @return the type name
     */
    public String type() {
        return type;
    }

    /**
     * Returns the mapping source.
     *
     * @return the source
     */
    public CompressedData source() {
        return source;
    }

    /**
     * Returns whether routing is required.
     *
     * @return true if routing is required
     */
    public boolean routingRequired() {
        return routingRequired;
    }

    /**
     * Writes this MappingMetadataModel to a stream.
     * The serialization format is compatible with MappingMetadata.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        source.writeTo(out);
        out.writeBoolean(routingRequired);
    }

    /**
     * Compares this MappingMetadataModel with another object for equality.
     * Two models are equal if all their fields are equal.
     *
     * @param o the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadataModel that = (MappingMetadataModel) o;

        return routingRequired == that.routingRequired && Objects.equals(type, that.type) && Objects.equals(source, that.source);
    }

    /**
     * Returns the hash code for this MappingMetadataModel.
     * The hash code is computed from all fields.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(type, source, routingRequired);
    }
}
