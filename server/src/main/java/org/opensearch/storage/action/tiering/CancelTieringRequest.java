/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to cancel an ongoing tiering operation for an index.
 * This request contains the index name and necessary parameters to safely
 * cancel migrations that may be stuck in RUNNING_SHARD_RELOCATION state.
 */
public class CancelTieringRequest extends AcknowledgedRequest<CancelTieringRequest> {

    private String index;

    /** Default constructor for stream deserialization. */
    public CancelTieringRequest() {
        super();
    }

    /**
     * Constructs a request for the given index.
     * @param index the index name
     */
    public CancelTieringRequest(final String index) {
        super();
        this.index = index;
    }

    /**
     * Constructs a request from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public CancelTieringRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null || index.isBlank()) {
            validationException = addValidationError("Index name cannot be null or empty", validationException);
        }
        return validationException;
    }

    /**
     * Gets the index name for which to cancel tiering.
     *
     * @return the index name
     */
    public String getIndex() {
        return index;
    }

    /**
     * Sets the index name for which to cancel tiering.
     *
     * @param index the index name
     */
    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CancelTieringRequest that = (CancelTieringRequest) o;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    @Override
    public String toString() {
        return "CancelTieringRequest{" + "index='" + index + '\'' + '}';
    }
}
