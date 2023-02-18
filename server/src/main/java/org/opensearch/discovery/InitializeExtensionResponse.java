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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.discovery;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.List;

/**
 * PluginResponse to intialize plugin
 *
 * @opensearch.internal
 */
public class InitializeExtensionResponse extends TransportResponse {
    private String name;
    private List<String> implementatedInterfaces;

    public InitializeExtensionResponse(String name, List<String> implementatedInterfaces) {
        this.name = name;
        this.implementatedInterfaces = implementatedInterfaces;
    }

    public InitializeExtensionResponse(StreamInput in) throws IOException {
        name = in.readString();
        int size = in.readVInt();
        this.implementatedInterfaces = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            String extensionInterfaceType = in.readString();
            this.implementatedInterfaces.add(extensionInterfaceType);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(implementatedInterfaces.size());
        for (String interfaceVal : implementatedInterfaces) {
            out.writeString(interfaceVal);
        }
    }

    /**
     * @return the node that is currently leading, according to the responding node.
     */

    public String getName() {
        return this.name;
    }

    /**
     * @return interfaces implemented by an extension
     */

    public List<String> getImplementedInterfaces() {
        return implementatedInterfaces;
    }

    @Override
    public String toString() {
        return "PluginResponse{" + "name" + name + "implementedInterfaces" + implementatedInterfaces + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitializeExtensionResponse that = (InitializeExtensionResponse) o;
        return Objects.equals(name, that.name) && Objects.equals(implementatedInterfaces, that.implementatedInterfaces);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, implementatedInterfaces);
    }
}
