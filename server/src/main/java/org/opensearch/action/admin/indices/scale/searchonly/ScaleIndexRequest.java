/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * A request for search-only scale operations (up or down) on an index.
 * <p>
 * This request represents an administrative operation to either:
 * <ul>
 *   <li>Scale an index down to search-only mode, removing write capability while preserving search replicas</li>
 *   <li>Scale an index up from search-only mode back to full read-write operation</li>
 * </ul>
 * <p>
 * The request is processed by the cluster manager node, which coordinates the necessary
 * cluster state changes, shard synchronization, and recovery operations needed to transition
 * an index between normal and search-only states.
 */
class ScaleIndexRequest extends AcknowledgedRequest<ScaleIndexRequest> {
    private final String index;
    private boolean scaleDown;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    /**
     * Constructs a new SearchOnlyRequest.
     *
     * @param index     the name of the index to scale
     * @param scaleDown true to scale down to search-only mode, false to scale up to normal operation
     */
    ScaleIndexRequest(String index, boolean scaleDown) {
        super();
        this.index = index;
        this.scaleDown = scaleDown;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if there is an I/O error during deserialization
     */
    ScaleIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.scaleDown = in.readBoolean();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    /**
     * Validates this request.
     * <p>
     * Ensures that the index name is provided and not empty.
     *
     * @return validation exception if invalid, null otherwise
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null || index.trim().isEmpty()) {
            validationException = ValidateActions.addValidationError("index is required", validationException);
        }
        return validationException;
    }

    /**
     * Serializes this request to the given output stream.
     *
     * @param out the output stream to write to
     * @throws IOException if there is an I/O error during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeBoolean(scaleDown);
        indicesOptions.writeIndicesOptions(out);
    }

    /**
     * Checks if this request equals another object.
     *
     * @param o the object to compare with
     * @return true if equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScaleIndexRequest that = (ScaleIndexRequest) o;
        return scaleDown == that.scaleDown && Objects.equals(index, that.index) && Objects.equals(indicesOptions, that.indicesOptions);
    }

    /**
     * Returns a hash code for this request.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(index, scaleDown, indicesOptions);
    }

    /**
     * Returns the name of the index to scale.
     *
     * @return the index name
     */
    public String getIndex() {
        return index;
    }

    /**
     * Returns whether this is a scale-down operation.
     *
     * @return true if scaling down to search-only mode, false if scaling up to normal operation
     */
    public boolean isScaleDown() {
        return scaleDown;
    }

    /**
     * Returns the indices options that specify how to resolve indices and handle unavailable indices.
     *
     * @return the indices options
     */
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Sets the indices options for this request.
     *
     * @param indicesOptions the indices options to use
     * @return this request (for method chaining)
     */
    public ScaleIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Sets whether this is a scale down operation.
     *
     * @param scaleDown true if scaling down, false if scaling up
     * @return this request (for method chaining)
     */
    public ScaleIndexRequest scaleDown(boolean scaleDown) {
        this.scaleDown = scaleDown;
        return this;
    }
}
