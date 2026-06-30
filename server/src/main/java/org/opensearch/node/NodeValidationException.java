/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.node;

import org.opensearch.core.common.transport.BoundTransportAddress;

import java.util.List;

/**
 * An exception thrown during node validation. Node validation runs immediately before a node
 * begins accepting network requests in
 * {@link Node#validateNodeBeforeAcceptingRequests(org.opensearch.bootstrap.BootstrapContext, BoundTransportAddress, List)}.
 * This exception is a checked exception that is declared as thrown from this method for the purpose of bubbling up to the user.
 *
 * @opensearch.internal
 */
public class NodeValidationException extends Exception {

    /**
     * Creates a node validation exception with the specified validation message to be displayed to
     * the user.
     *
     * @param message the message to display to the user
     */
    public NodeValidationException(final String message) {
        super(message);
    }

}
