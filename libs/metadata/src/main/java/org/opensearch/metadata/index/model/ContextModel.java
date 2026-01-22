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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A model class for Context metadata that can be used in metadata lib without
 * depending on the full Context class from server module.
 */
@ExperimentalApi
public final class ContextModel implements Writeable {

    private final String name;
    private final String version;
    private final Map<String, Object> params;

    /**
     * Creates a ContextModel with the given fields.
     *
     * @param name the context name
     * @param version the context version
     * @param params the context parameters
     */
    public ContextModel(String name, String version, Map<String, Object> params) {
        this.name = name;
        this.version = version;
        this.params = params;
    }

    /**
     * Creates a ContextModel by reading from StreamInput.
     * Wire format is compatible with Context(StreamInput).
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public ContextModel(StreamInput in) throws IOException {
        this.name = in.readString();
        this.version = in.readOptionalString();
        this.params = in.readMap();
    }

    /**
     * Writes to StreamOutput.
     * Wire format is compatible with Context.writeTo.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(version);
        out.writeMap(params);
    }

    /**
     * Returns the context name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the context version.
     *
     * @return the version
     */
    public String version() {
        return version;
    }

    /**
     * Returns the context parameters.
     *
     * @return the params map
     */
    public Map<String, Object> params() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContextModel that = (ContextModel) o;
        return Objects.equals(name, that.name) && Objects.equals(version, that.version) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version, params);
    }
}
