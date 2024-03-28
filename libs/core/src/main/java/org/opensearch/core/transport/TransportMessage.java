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

package org.opensearch.core.transport;

import org.opensearch.core.common.io.stream.BytesWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.transport.TransportAddress;

import java.io.InputStream;

/**
 * Message over the transport interface
 *
 * @opensearch.internal
 */
public abstract class TransportMessage implements Writeable, BytesWriteable {

    private TransportAddress remoteAddress;

    private String protocol;

    public void remoteAddress(TransportAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public TransportAddress remoteAddress() {
        return remoteAddress;
    }

    public String getProtocol() {
        if (protocol != null) {
            return protocol;
        }
        return "native";
    }

    /**
     * Constructs a new empty transport message
     */
    public TransportMessage() {}

    /**
     * Constructs a new transport message with the data from the {@link StreamInput}. This is
     * currently a no-op
     */
    public TransportMessage(StreamInput in) {}

    /**
    * Constructs a new transport message with the data from the byte array. This is
    * currently a no-op
    */
    public TransportMessage(InputStream in) {}
}
