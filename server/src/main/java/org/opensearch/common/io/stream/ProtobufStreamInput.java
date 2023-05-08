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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.apache.lucene.util.ArrayUtil;

import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;

/**
 * A class for additional methods to read from a {@link CodedInputStream}.
 */
public class ProtobufStreamInput {

    private Version version = Version.CURRENT;

    private static final TimeUnit[] TIME_UNITS = TimeUnit.values();

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

    @Nullable
    public Long readOptionalLong(CodedInputStream in) throws IOException {
        if (readBoolean(in)) {
            return in.readInt64();
        }
        return null;
    }

    @Nullable
    public final Boolean readOptionalBoolean(CodedInputStream in) throws IOException {
        final byte value = in.readRawByte();
        if (value == 2) {
            return null;
        } else {
            return readBoolean(value);
        }
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

    /**
     * Read a {@link TimeValue} from the stream
     */
    public TimeValue readTimeValue(CodedInputStream in) throws IOException {
        long duration = in.readInt64();
        TimeUnit timeUnit = TIME_UNITS[in.readRawByte()];
        return new TimeValue(duration, timeUnit);
    }

    public String[] readStringArray(CodedInputStream in) throws IOException {
        int size = readArraySize(in);
        if (size == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readString();
        }
        return ret;
    }

    private <E extends Enum<E>> E readEnum(Class<E> enumClass, E[] values, CodedInputStream in) throws IOException {
        int ordinal = readVInt(in);
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IOException("Unknown " + enumClass.getSimpleName() + " ordinal [" + ordinal + "]");
        }
        return values[ordinal];
    }

    /**
     * Reads an enum with type E that was serialized based on the value of it's ordinal
     */
    public <E extends Enum<E>> EnumSet<E> readEnumSet(Class<E> enumClass, CodedInputStream in) throws IOException {
        int size = readVInt(in);
        final EnumSet<E> res = EnumSet.noneOf(enumClass);
        if (size == 0) {
            return res;
        }
        final E[] values = enumClass.getEnumConstants();
        for (int i = 0; i < size; i++) {
            res.add(readEnum(enumClass, values, in));
        }
        return res;
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

    /**
     * Reads a list of objects. The list is expected to have been written using {@link StreamOutput#writeList(List)}.
     * If the returned list contains any entries it will be mutable. If it is empty it might be immutable.
     *
     * @return the list of objects
     * @throws IOException if an I/O exception occurs reading the list
     */
    public <T> List<T> readList(final ProtobufWriteable.Reader<T> reader, CodedInputStream in) throws IOException {
        return readCollection(reader, ArrayList::new, Collections.emptyList(), in);
    }

    /**
     * Reads a collection of objects
     */
    private <T, C extends Collection<? super T>> C readCollection(
        ProtobufWriteable.Reader<T> reader,
        IntFunction<C> constructor,
        C empty,
        CodedInputStream in
    ) throws IOException {
        int count = readArraySize(in);
        if (count == 0) {
            return empty;
        }
        C builder = constructor.apply(count);
        for (int i = 0; i < count; i++) {
            builder.add(reader.read(in));
        }
        return builder;
    }

    /**
     * Reads a {@link NamedWriteable} from the current stream, by first reading its name and then looking for
     * the corresponding entry in the registry by name, so that the proper object can be read and returned.
     * Default implementation throws {@link UnsupportedOperationException} as StreamInput doesn't hold a registry.
     */
    @Nullable
    public <C extends ProtobufNamedWriteable> C readNamedWriteable(@SuppressWarnings("unused") Class<C> categoryClass) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    /**
     * Reads a {@link ProtobufNamedWriteable} from the current stream with the given name. It is assumed that the caller obtained the name
     * from other source, so it's not read from the stream. The name is used for looking for
     * the corresponding entry in the registry by name, so that the proper object can be read and returned.
     * Default implementation throws {@link UnsupportedOperationException} as StreamInput doesn't hold a registry.
     */
    @Nullable
    public <C extends ProtobufNamedWriteable> C readNamedWriteable(
        @SuppressWarnings("unused") Class<C> categoryClass,
        @SuppressWarnings("unused") String name
    ) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    public <T> T[] readOptionalArray(ProtobufWriteable.Reader<T> reader, IntFunction<T[]> arraySupplier, CodedInputStream in)
        throws IOException {
        return readBoolean(in) ? readArray(reader, arraySupplier, in) : null;
    }

    public <T> T[] readArray(final ProtobufWriteable.Reader<T> reader, final IntFunction<T[]> arraySupplier, CodedInputStream in)
        throws IOException {
        final int length = readArraySize(in);
        final T[] values = arraySupplier.apply(length);
        for (int i = 0; i < length; i++) {
            values[i] = reader.read(in);
        }
        return values;
    }

    /**
     * Read an optional {@link TimeValue} from the stream, returning null if no TimeValue was written.
     */
    public @Nullable TimeValue readOptionalTimeValue(CodedInputStream in) throws IOException {
        if (readBoolean(in)) {
            return readTimeValue(in);
        } else {
            return null;
        }
    }

}
