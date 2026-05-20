/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.OpenSearchException;

/**
 * Thrown when transfer event is rejected due to breach in event queue size.
 */
public class S3TransferRejectedException extends OpenSearchException {
    public S3TransferRejectedException(String msg) {
        super(msg);
    }
}
