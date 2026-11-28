/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * A request that must remain in memory on the receiving node.
 * Subclasses still provide normal request validation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class LocalOnlyActionRequest extends ActionRequest {
    @Override
    public final void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("Request [" + getClass().getName() + "] is local-only");
    }
}
