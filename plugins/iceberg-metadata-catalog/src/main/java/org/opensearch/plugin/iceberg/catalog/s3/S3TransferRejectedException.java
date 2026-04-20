/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.catalog.s3;
// Copied from plugins/repository-s3/ for plugin isolation. Tech debt: extract to shared library.

import org.opensearch.OpenSearchException;

/**
 * Thrown when transfer event is rejected due to breach in event queue size.
 */
public class S3TransferRejectedException extends OpenSearchException {
    public S3TransferRejectedException(String msg) {
        super(msg);
    }
}
