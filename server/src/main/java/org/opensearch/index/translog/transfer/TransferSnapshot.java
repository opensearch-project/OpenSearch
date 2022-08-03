/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.FileSnapshot;

import java.util.Set;

public interface TransferSnapshot {

    Set<FileSnapshot> getCheckpointFileSnapshots();

    Set<FileSnapshot> getTranslogFileSnapshots();

    int getTransferSize();
}
