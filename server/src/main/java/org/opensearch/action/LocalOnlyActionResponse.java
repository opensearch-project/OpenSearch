/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * An in-memory action response. REST responses can still implement
 * {@link org.opensearch.core.xcontent.ToXContentObject} for JSON rendering.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class LocalOnlyActionResponse extends ActionResponse {
    @Override
    public final void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("Response [" + getClass().getName() + "] is local-only");
    }
}
