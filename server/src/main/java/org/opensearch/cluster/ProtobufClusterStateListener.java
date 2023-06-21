/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

/**
 * A listener to be notified when a cluster state changes.
 *
 * @opensearch.internal
 */
public interface ProtobufClusterStateListener {

    /**
     * Called when cluster state changes.
     */
    void clusterChanged(ProtobufClusterChangedEvent event);
}
