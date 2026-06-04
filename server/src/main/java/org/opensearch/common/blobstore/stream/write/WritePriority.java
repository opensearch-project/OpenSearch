/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.write;

/**
 * WritePriority for upload
 *
 * @opensearch.internal
 */
public enum WritePriority {
    // Used for segment transfers during refresh, flush or merges
    NORMAL,
    // Used for transfer of translog or ckp files.
    HIGH,
    // Used for transfer of remote cluster state
    URGENT,
    // All other background transfers such as in snapshot recovery, recovery from local store or index etc.
    LOW
}
