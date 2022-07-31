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
import java.util.List;
import java.util.Objects;

/**
 * PluginResponse to intialize plugin
 *
 * @opensearch.internal
 */
public class PluginResponse extends TransportResponse {
    private String name;
    private List<String> api;

    public PluginResponse(String name, List<String> api) {
        this.name = name;
        this.api = new ArrayList<>(api);
    }

    public PluginResponse(StreamInput in) throws IOException {
        name = in.readString();
        api = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(api);
    }

    /**
     * @return the node that is currently leading, according to the responding node.
     */
    public String getName() {
        return this.name;
    }

    public List<String> getApi() {
        return new ArrayList<>(api);
    }

    @Override
    public String toString() {
        return "PluginResponse {" + "name=" + name + ", api=" + api + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginResponse that = (PluginResponse) o;
        return Objects.equals(name, that.name) && Objects.equals(api, that.api);
    }

    @Override
    public int hashCode() {
        return Objects.hash(api, name);
    }

}
