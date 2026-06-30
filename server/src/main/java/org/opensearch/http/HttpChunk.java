/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.common.bytes.BytesReference;

/**
 * Represents a chunk of the HTTP request / response stream
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface HttpChunk extends Releasable {
    /**
     * Signals this is the last chunk of the stream.
     * @return "true" if this is the last chunk of the stream, "false" otherwise
     */
    boolean isLast();

    /**
    * Returns the content of this chunk
    * @return the content of this chunk
    */
    BytesReference content();
}
