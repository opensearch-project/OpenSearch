/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

/**
 * Manifest of diff between two cluster states
 *
 * @opensearch.internal
 */
public class ClusterStateDiffManifest {

    // TODO https://github.com/opensearch-project/OpenSearch/pull/14089
    public String getFromStateUUID() {
        return null;
    }
}
