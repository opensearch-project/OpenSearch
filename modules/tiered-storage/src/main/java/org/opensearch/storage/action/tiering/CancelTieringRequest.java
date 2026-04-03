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

import java.io.IOException;

/**
 * Request to cancel an ongoing tiering operation for an index.
 * This request contains the index name and necessary parameters to safely
 * cancel migrations that may be stuck in RUNNING_SHARD_RELOCATION state.
 *
 * Serialization (writeTo, StreamInput constructor), validation, equals/hashCode, and toString
 * will be added in the implementation PR.
 */
public class CancelTieringRequest extends AcknowledgedRequest<CancelTieringRequest> {

    private String index;

    /** Default constructor. */
    public CancelTieringRequest() {
        super();
    }

    /**
     * Constructs a request from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public CancelTieringRequest(StreamInput in) throws IOException {
        super(in);
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Constructs a request for the given index.
     * @param index the index name
     */
    public CancelTieringRequest(final String index) {
        super();
        this.index = index;
    }

    @Override
    public ActionRequestValidationException validate() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /** Returns the index name. */
    public String getIndex() {
        return index;
    }

    /**
     * Sets the index name.
     * @param index the index name
     */
    public void setIndex(String index) {
        this.index = index;
    }
}
