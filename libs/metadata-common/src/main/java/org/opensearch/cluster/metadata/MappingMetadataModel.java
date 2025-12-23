/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Pure data model for mapping metadata.
 * Can be used independently or converted to/from {MappingMetadata}.
 * <p>
 * This class contains only the essential data fields:
 * <ul>
 *   <li>type - the mapping type name</li>
 *   <li>source - the mapping source</li>
 *   <li>routingRequired - whether routing is required</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.4.0")
public final class MappingMetadataModel implements Writeable {

    private final String type;
    private final String source;
    private final boolean routingRequired;

    /**
     * Creates a new MappingMetadataModel.
     *
     * @param type the mapping type name
     * @param source the compressed mapping source
     * @param routingRequired whether routing is required
     */
    public MappingMetadataModel(String type, String source, boolean routingRequired) {
        this.type = type;
        this.source = source;
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
        this.source = in.readString();
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
    public String source() {
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(source);
        out.writeBoolean(routingRequired);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadataModel that = (MappingMetadataModel) o;

        return routingRequired == that.routingRequired && Objects.equals(type, that.type) && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, source, routingRequired);
    }
}
