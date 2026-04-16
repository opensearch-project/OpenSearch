/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.model;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;

/**
 * Tiering status request for all indices.
 * OR
 * Tiering status request for indices for specific Tiering State:
 * eg. "_tier/all" - for all tiering indices
 * eg. "_tier/all?target=_warm" - for Hot to Warm Tiering
 * eg. "_tier/all?target=_hot" - for Warm to Hot Tiering
 */
public class ListTieringStatusRequest extends ClusterManagerNodeReadRequest<ListTieringStatusRequest> {

    private String targetTier;

    /** Returns the target tier. */
    public String getTargetTier() {
        return targetTier;
    }

    /** Constructs a default request with no target tier. */
    public ListTieringStatusRequest() {
        this.targetTier = null;
    }

    /**
     * Constructs a request for the given target tier.
     * @param targetTier the target tier
     */
    public ListTieringStatusRequest(String targetTier) {
        if (targetTier != null) {
            this.targetTier = targetTier.toUpperCase(Locale.ROOT);
        } else {
            this.targetTier = null;
        }
    }

    /**
     * Constructs a request from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public ListTieringStatusRequest(StreamInput in) throws IOException {
        super(in);
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
