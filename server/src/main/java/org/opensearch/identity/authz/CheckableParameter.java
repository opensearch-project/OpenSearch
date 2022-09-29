/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Parameter in an {@link AuthorizationRequest}
 *
 * CheckableParameters are parameters needed when performing an authorization request such as a resource identifier or
 * an index pattern.
 *
 * @opensearch.experimental
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
        // Potential performance improvement for Class.forName(...) - https://stackoverflow.com/q/18231991/533057
        return new CheckableParameter(key, value, Class.forName(className));
    }

    public static void writeParameterToStream(CheckableParameter param, StreamOutput out) throws IOException {
        out.writeString(param.getKey());
        out.writeString(param.getType().getName());
        out.writeGenericValue(param.getValue());
    }

    @Override
    public String toString() {
        return "CheckableParameter{key=" + key + ", value=" + value + ", type=" + type + "}";
    }
}
