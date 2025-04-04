/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchWrapperException;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception thrown when there is an error in the ingestion engine.
 *
 * @opensearch.internal
 */
public class IngestionEngineException extends OpenSearchException implements OpenSearchWrapperException {
    public IngestionEngineException(Throwable cause) {
        super(cause);
    }

    public IngestionEngineException(StreamInput in) throws IOException {
        super(in);
    }
}
