/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.ActionListener;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.io.IOException;


public interface TransferService {

    void uploadFileAsync(final TransferFileSnapshot fileSnapshot, Iterable<String> remotePath, ActionListener<TransferFileSnapshot> listener);

    void uploadFile(final TransferFileSnapshot fileSnapshot, Iterable<String> remotePath) throws IOException;

}
