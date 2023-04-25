/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common.io.stream;

import com.google.protobuf.CodedInputStream;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.util.ArrayUtil;

import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.Version;
import org.opensearch.common.Nullable;

/**
 * A class for additional methods to read from a {@link CodedInputStream}.
 */
public class ProtobufStreamInput {

    private Version version = Version.CURRENT;

    /**
     * The version of the node on the other side of this stream.
     */
    public Version getVersion() {
        return this.version;
    }

    /**
     * Set the version of the node on the other side of this stream.
     */
    public void setVersion(Version version) {
        this.version = version;
    }

    @Nullable
    public String readOptionalString(CodedInputStream in) throws IOException {
        if (readBoolean(in)) {
            return in.readString();
        }
        return null;
    }

    /**
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    public <K, V> Map<K, V> readMap(ProtobufWriteable.Reader<K> keyReader, ProtobufWriteable.Reader<V> valueReader, CodedInputStream in)
        throws IOException {
        int size = readArraySize(in);
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<K, V> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            K key = keyReader.read(in);
            V value = valueReader.read(in);
            map.put(key, value);
        }
        return map;
    }

    @Nullable
    public <T extends ProtobufWriteable> T readOptionalWriteable(ProtobufWriteable.Reader<T> reader, CodedInputStream in)
        throws IOException {
        if (readBoolean(in)) {
            T t = reader.read(in);
            if (t == null) {
                throw new IOException(
                    "Writeable.Reader [" + reader + "] returned null which is not allowed and probably means it screwed up the stream."
                );
            }
            return t;
        } else {
            return null;
        }
    }

    private int readArraySize(CodedInputStream in) throws IOException {
        final int arraySize = readVInt(in);
        if (arraySize > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalStateException("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH + " but was: " + arraySize);
        }
        if (arraySize < 0) {
            throw new NegativeArraySizeException("array size must be positive but was: " + arraySize);
        }
        // lets do a sanity check that if we are reading an array size that is bigger that the remaining bytes we can safely
        // throw an exception instead of allocating the array based on the size. A simple corrutpted byte can make a node go OOM
        // if the size is large and for perf reasons we allocate arrays ahead of time
        // ensureCanReadBytes(arraySize);
        return arraySize;
    }

    public int readVInt(CodedInputStream in) throws IOException {
        byte b = in.readRawByte();
        int i = b & 0x7F;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = in.readRawByte();
        i |= (b & 0x7F) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = in.readRawByte();
        i |= (b & 0x7F) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = in.readRawByte();
        i |= (b & 0x7F) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = in.readRawByte();
        if ((b & 0x80) != 0) {
            throw new IOException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(i));
        }
        return i | ((b & 0x7F) << 28);
    }

    /**
     * Reads a boolean.
     */
    public final boolean readBoolean(CodedInputStream in) throws IOException {
        return readBoolean(in.readRawByte());
    }

    /**
     * Reads an optional bytes reference from this stream. It might hold an actual reference to the underlying bytes of the stream. Use this
     * only if you must differentiate null from empty.
     */
    @Nullable
    public BytesReference readOptionalBytesReference(CodedInputStream in) throws IOException {
        int length = readVInt(in) - 1;
        if (length < 0) {
            return null;
        }
        return readBytesReference(length, in);
    }

    /**
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference(int length, CodedInputStream in) throws IOException {
        if (length == 0) {
            return BytesArray.EMPTY;
        }
        byte[] bytes = new byte[length];
        bytes = in.readByteArray();
        return new BytesArray(bytes, 0, length);
    }

    private boolean readBoolean(final byte value) {
        if (value == 0) {
            return false;
        } else if (value == 1) {
            return true;
        } else {
            final String message = String.format(Locale.ROOT, "unexpected byte [0x%02x]", value);
            throw new IllegalStateException(message);
        }
    }
}
