/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.serde;

import com.google.protobuf.ByteString;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.awt.image.WritableRaster;
import java.io.IOException;

/**
 * Base class for supported serialization/deserialization implementations.
 * @opensearch.internal
 */
public class SerDe {

    /**
     * Serialization/Deserialization exception.
     * @opensearch.internal
     */
    public static class SerializationException extends RuntimeException {
        public SerializationException(String message) {
            super(message);
        }

        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }

        public SerializationException(Throwable cause) {
            super(cause);
        }
    }

    interface XContentSerializer<T> {
        public XContentBuilder serialize(T object) throws SerializationException;
    }

    interface XContentDeserializer<V> {
        public V deserialize(XContentParser parser) throws SerializationException;
    }

    interface StreamSerializer<T> {
        public void serialize(T object, StreamOutput out) throws SerializationException;
    }

    interface StreamDeserializer<V> {
        public V deserialize(StreamInput in) throws SerializationException;
    }
}
