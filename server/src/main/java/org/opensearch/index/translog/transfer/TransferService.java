/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.ActionListener;

import java.io.File;
import java.io.IOException;

public interface TransferService {

    void uploadFile(final File file, RemotePathProvider remotePathProvider, ActionListener<Void> listener) throws IOException;

}
