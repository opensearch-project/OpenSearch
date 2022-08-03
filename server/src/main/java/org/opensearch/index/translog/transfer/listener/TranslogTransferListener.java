/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.listener;

import org.opensearch.index.translog.transfer.TransferSnapshot;

public interface TranslogTransferListener {

    void onUploadComplete(TransferSnapshot transferSnapshot);

    void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex);
}
