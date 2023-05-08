/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.client;

import org.opensearch.action.ActionListener;

/**
 * A client provides a one stop interface for performing actions/operations against the cluster.
* <p>
* All operations performed are asynchronous by nature. Each action/operation has two flavors, the first
* simply returns an {@link org.opensearch.action.ActionFuture}, while the second accepts an
* {@link ActionListener}.
* <p>
* A client can be retrieved from a started {@link org.opensearch.node.Node}.
*
* @see org.opensearch.node.Node#client()
*
* @opensearch.internal
*/
public interface ProtobufClient extends ProtobufOpenSearchClient {

    /**
     * The admin client that can be used to perform administrative operations.
    */
    ProtobufAdminClient admin();
}
