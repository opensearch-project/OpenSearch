/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

@ExperimentalApi()
public sealed interface ExtraFieldValue permits FloatArrayValue, BytesValue {

    @ExperimentalApi
    enum Type {
        BYTES((byte) 0, BytesValue::readBodyFrom),
        FLOAT_ARRAY((byte) 1, FloatArrayValue::readBodyFrom);
        // TODO add double, int and long

        private final byte id;
        private final Reader reader;

        Type(byte id, Reader reader) {
            this.id = id;
            this.reader = reader;
        }

        byte id() {
            return id;
        }

        ExtraFieldValue readBody(StreamInput in) throws IOException {
            return reader.read(in);
        }

        private static final Type[] LOOKUP = new Type[256];
        static {
            for (Type t : values()) {
                LOOKUP[t.id & 0xFF] = t;
            }
        }

        static Type fromId(byte id) {
            final Type t = LOOKUP[id & 0xFF];
            if (t == null) {
                throw new IllegalArgumentException("Unknown ExtraFieldValue.Type id: " + id);
            }
            return t;
        }

        @FunctionalInterface
        interface Reader {
            ExtraFieldValue read(StreamInput in) throws IOException;
        }
    }

    Type type();

    /**
     * For arrays: element count (dimension).
     * For bytes: byte length.
     */
    int size();

    /**
     * Write "body only" (no type id). Container calls {@link #writeTo(StreamOutput)}.
     */
    void writeBodyTo(StreamOutput out) throws IOException;

    /**
     * Default wire encoding: [typeId][body...]
     */
    default void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type().id());
        writeBodyTo(out);
    }

    /**
     * Decode: reads [id] then dispatches to the appropriate body reader.
     */
    static ExtraFieldValue readFrom(StreamInput in) throws IOException {
        return Type.fromId(in.readByte()).readBody(in);
    }
}
