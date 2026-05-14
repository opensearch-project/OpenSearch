/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.OpenSearchException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown when a merge cannot proceed because the target file system does not have
 * enough free space to hold the projected merged segment output (with a configured
 * safety margin).
 *
 * <p>This is a hard failure. Disk space does not magically reappear, so the merge
 * is aborted and the source segments remain available for a future merge attempt
 * once space is reclaimed (e.g., via deletes, snapshots, or operator action).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InsufficientDiskSpaceException extends OpenSearchException {

    public InsufficientDiskSpaceException(String message) {
        super(message);
    }

    public InsufficientDiskSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public InsufficientDiskSpaceException(StreamInput in) throws IOException {
        super(in);
    }
}
