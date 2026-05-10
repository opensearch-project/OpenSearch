/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Thrown when a composite write partially fails and the writer has been internally
 * reconciled (rolled back). The writer contains N-1 consistent documents and should
 * be flushed and closed to produce a clean segment.
 * <p>
 * The engine should treat this as a document-level failure (fail the request, not the engine)
 * and signal the writer pool to flush and close the current writer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FlushAndCloseWriterException extends Exception {

    public FlushAndCloseWriterException(String message, Exception cause) {
        super(message, cause);
    }
}
