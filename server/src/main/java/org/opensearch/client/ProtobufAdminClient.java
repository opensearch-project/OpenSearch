/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.client;

/**
 * Administrative actions/operations against the cluster or the indices.
*
* @see org.opensearch.client.Client#admin()
*
* @opensearch.internal
*/
public interface ProtobufAdminClient {

    /**
     * A client allowing to perform actions/operations against the cluster.
    */
    ProtobufClusterAdminClient cluster();
}
