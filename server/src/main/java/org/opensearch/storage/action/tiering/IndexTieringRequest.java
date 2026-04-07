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
 * Represents a request to move indices between different storage tiers.
 * This class handles the validation and serialization of tiering requests.
 * Tier field (TieringUtils.Tier) will be added in the implementation PR.
 *
 * Serialization (writeTo, StreamInput constructor), validation, equals/hashCode
 * will be added in the implementation PR.
 */
public class IndexTieringRequest extends AcknowledgedRequest<IndexTieringRequest> {

    private final String indexName;

    /**
     * Constructs a new tiering request from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public IndexTieringRequest(StreamInput in) throws IOException {
        super(in);
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Constructs a new tiering request.
     * @param targetTier the target tier
     * @param indexName the index name
     */
    public IndexTieringRequest(final String targetTier, final String indexName) {
        this.indexName = indexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // tier() method returning TieringUtils.Tier will be added in the implementation PR.

    /** Returns the index name. */
    public String getIndex() {
        return indexName;
    }
}
