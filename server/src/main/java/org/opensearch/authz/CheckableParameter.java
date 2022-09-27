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

package org.opensearch.authz;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Parameter in an {@link AuthorizationRequest}
 *
 * @opensearch.internal
 */
public class CheckableParameter<T> {

    private final String key;
    private final T value;

    private Class<T> type;

    public CheckableParameter(String key, T value, Class<T> type) {
        this.key = key;
        this.value = value;
        this.type = type;
    }

    public String getKey() {
        return this.key;
    }

    public T getValue() {
        return this.value;
    }

    public Class<T> getType() {
        return this.type;
    }

    public static CheckableParameter readParameterFromStream(StreamInput in) throws IOException, ClassNotFoundException {
        String key = in.readString();
        String className = in.readString();
        Object value = in.readGenericValue();
        return new CheckableParameter(key, value, Class.forName(className));
    }

    public static void writeParameterToStream(CheckableParameter param, StreamOutput out) throws IOException {
        out.writeString(param.getKey());
        out.writeString(param.getType().getName());
        out.writeGenericValue(param.getValue());
    }

    @Override
    public String toString() {
        return "CheckableParameter{key=" + key + ", value=" + value + "}";
    }
}
