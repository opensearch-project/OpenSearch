/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.stream;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Interface for server module classes that need to participate in metadata model serialization.
 * <p>
 * This allows classes that cannot move to libs/metadata (due to server dependencies) to be used within model classes.
 * Server classes implement this interface and provide a {@link MetadataReader} for deserialization.
 */
public interface MetadataWriteable extends Writeable {

    /**
     * Write this object to the metadata stream.
     * Implementations should delegate to their existing writeTo method.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    void writeToMetadataStream(StreamOutput out) throws IOException;

    /**
     * Functional interface for reading MetadataWriteable objects from a stream.
     * <p>
     * This is used by model classes to deserialize server module classes without
     * having a direct dependency on them.
     *
     * @param <T> the type of MetadataWriteable to read
     */
    @FunctionalInterface
    interface MetadataReader<T extends MetadataWriteable> {
        /**
         * Read an object from the metadata stream.
         *
         * @param in the stream to read from
         * @return the deserialized object
         * @throws IOException if an I/O error occurs
         */
        T readFromMetadataStream(StreamInput in) throws IOException;
    }
}
