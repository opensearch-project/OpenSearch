/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.cluster;

import com.google.protobuf.CodedInputStream;
import org.opensearch.Version;
import org.opensearch.common.io.stream.ProtobufStreamInput;

import java.io.IOException;

/**
 * Value Serializer for named diffables
*
* @opensearch.internal
*/
public class ProtobufNamedDiffableValueSerializer<T extends ProtobufNamedDiffable<T>> extends ProtobufDiffableUtils.DiffableValueSerializer<
    String,
    T> {

    private final Class<T> tClass;

    public ProtobufNamedDiffableValueSerializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public T read(CodedInputStream in, String key) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput();
        return protobufStreamInput.readNamedWriteable(tClass, key);
    }

    @Override
    public boolean supportsVersion(ProtobufDiff<T> value, Version version) {
        return version.onOrAfter(((ProtobufNamedDiff<?>) value).getMinimalSupportedVersion());
    }

    @Override
    public boolean supportsVersion(T value, Version version) {
        return version.onOrAfter(value.getMinimalSupportedVersion());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProtobufDiff<T> readDiff(CodedInputStream in, String key) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput();
        return protobufStreamInput.readNamedWriteable(ProtobufNamedDiff.class, key);
    }
}
