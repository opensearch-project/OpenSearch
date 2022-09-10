/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.listener;

import org.opensearch.index.translog.transfer.TransferSnapshot;

import java.io.IOException;

/**
 * The listener to be invoked on the completion or failure of a {@link TransferSnapshot}
 *
 * @opensearch.internal
 */
public interface TranslogTransferListener {

    void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException;

    void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException;
}
