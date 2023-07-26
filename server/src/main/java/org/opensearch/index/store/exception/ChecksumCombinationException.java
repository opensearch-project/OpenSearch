/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.exception;

import org.apache.lucene.index.CorruptIndexException;

/**
 * Exception is raised when combination of two CRC checksums fail.
 *
 * @opensearch.internal
 */
public class ChecksumCombinationException extends CorruptIndexException {
    public ChecksumCombinationException(String msg, String resourceDescription) {
        super(msg, resourceDescription);
    }

    public ChecksumCombinationException(String msg, String resourceDescription, Throwable cause) {
        super(msg, resourceDescription, cause);
    }
}
