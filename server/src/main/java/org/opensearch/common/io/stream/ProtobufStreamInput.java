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
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
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
import org.opensearch.ProtobufOpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;

import static org.opensearch.ProtobufOpenSearchException.readStackTrace;

/**
 * A class for additional methods to read from a {@link CodedInputStream}.
 */
public class ProtobufStreamInput {

    private CodedInputStream in;
    private Version version = Version.CURRENT;

    private static final TimeUnit[] TIME_UNITS = TimeUnit.values();

    public ProtobufStreamInput(CodedInputStream in) {
        this.in = in;
    }

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
    public String readOptionalString() throws IOException {
        if (readBoolean()) {
            return this.in.readString();
        }
        return null;
    }

    @Nullable
    public Long readOptionalLong() throws IOException {
        if (readBoolean()) {
            return this.in.readInt64();
        }
        return null;
    }

    @Nullable
    public final Boolean readOptionalBoolean() throws IOException {
        final byte value = this.in.readRawByte();
        if (value == 2) {
            return null;
        } else {
            return readBoolean(value);
        }
    }

    /**
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    public <K, V> Map<K, V> readMap(ProtobufWriteable.Reader<K> keyReader, ProtobufWriteable.Reader<V> valueReader) throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<K, V> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            K key = keyReader.read(this.in);
            V value = valueReader.read(this.in);
            map.put(key, value);
        }
        return map;
    }

    @Nullable
    public <T extends ProtobufWriteable> T readOptionalWriteable(ProtobufWriteable.Reader<T> reader) throws IOException {
        if (readBoolean()) {
            T t = reader.read(this.in);
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

    private int readArraySize() throws IOException {
        final int arraySize = readVInt();
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

    public int readVInt() throws IOException {
        byte b = this.in.readRawByte();
        int i = b & 0x7F;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = this.in.readRawByte();
        i |= (b & 0x7F) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = this.in.readRawByte();
        i |= (b & 0x7F) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = this.in.readRawByte();
        i |= (b & 0x7F) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = this.in.readRawByte();
        if ((b & 0x80) != 0) {
            throw new IOException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(i));
        }
        return i | ((b & 0x7F) << 28);
    }

    /**
     * Reads a boolean.
     */
    public final boolean readBoolean() throws IOException {
        return readBoolean(this.in.readRawByte());
    }

    /**
     * Reads an optional bytes reference from this stream. It might hold an actual reference to the underlying bytes of the stream. Use this
     * only if you must differentiate null from empty.
     */
    @Nullable
    public BytesReference readOptionalBytesReference() throws IOException {
        int length = readVInt() - 1;
        if (length < 0) {
            return null;
        }
        return readBytesReference(length);
    }

    /**
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference() throws IOException {
        int length = readArraySize();
        return readBytesReference(length);
    }

    /**
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference(int length) throws IOException {
        if (length == 0) {
            return BytesArray.EMPTY;
        }
        byte[] bytes = new byte[length];
        bytes = this.in.readByteArray();
        return new BytesArray(bytes, 0, length);
    }

    /**
     * Read a {@link TimeValue} from the stream
     */
    public TimeValue readTimeValue() throws IOException {
        long duration = this.in.readInt64();
        TimeUnit timeUnit = TIME_UNITS[this.in.readRawByte()];
        return new TimeValue(duration, timeUnit);
    }

    public String[] readStringArray() throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = this.in.readString();
        }
        return ret;
    }

    /**
    * Reads an enum with type E that was serialized based on the value of its ordinal
    */
    public <E extends Enum<E>> E readEnum(Class<E> enumClass) throws IOException {
        return readEnum(enumClass, enumClass.getEnumConstants());
    }

    private <E extends Enum<E>> E readEnum(Class<E> enumClass, E[] values) throws IOException {
        int ordinal = readVInt();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IOException("Unknown " + enumClass.getSimpleName() + " ordinal [" + ordinal + "]");
        }
        return values[ordinal];
    }

    /**
     * Reads an enum with type E that was serialized based on the value of it's ordinal
     */
    public <E extends Enum<E>> EnumSet<E> readEnumSet(Class<E> enumClass) throws IOException {
        int size = readVInt();
        final EnumSet<E> res = EnumSet.noneOf(enumClass);
        if (size == 0) {
            return res;
        }
        final E[] values = enumClass.getEnumConstants();
        for (int i = 0; i < size; i++) {
            res.add(readEnum(enumClass, values));
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
    public <T> List<T> readList(final ProtobufWriteable.Reader<T> reader) throws IOException {
        return readCollection(reader, ArrayList::new, Collections.emptyList());
    }

    /**
     * Reads a collection of objects
     */
    private <T, C extends Collection<? super T>> C readCollection(ProtobufWriteable.Reader<T> reader, IntFunction<C> constructor, C empty)
        throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return empty;
        }
        C builder = constructor.apply(count);
        for (int i = 0; i < count; i++) {
            builder.add(reader.read(this.in));
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

    public <T> T[] readOptionalArray(ProtobufWriteable.Reader<T> reader, IntFunction<T[]> arraySupplier) throws IOException {
        return readBoolean() ? readArray(reader, arraySupplier) : null;
    }

    public <T> T[] readArray(final ProtobufWriteable.Reader<T> reader, final IntFunction<T[]> arraySupplier) throws IOException {
        final int length = readArraySize();
        final T[] values = arraySupplier.apply(length);
        for (int i = 0; i < length; i++) {
            values[i] = reader.read(this.in);
        }
        return values;
    }

    /**
     * Read an optional {@link TimeValue} from the stream, returning null if no TimeValue was written.
     */
    public @Nullable TimeValue readOptionalTimeValue() throws IOException {
        if (readBoolean()) {
            return readTimeValue();
        } else {
            return null;
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <T extends Exception> T readException() throws IOException {
        if (readBoolean()) {
            int key = readVInt();
            switch (key) {
                case 0:
                    final int ord = readVInt();
                    return (T) ProtobufOpenSearchException.readException(this.in, ord);
                case 4:
                    return (T) readStackTrace(new NullPointerException(readOptionalString()), this.in);
                case 5:
                    return (T) readStackTrace(new NumberFormatException(readOptionalString()), this.in);
                case 6:
                    return (T) readStackTrace(new IllegalArgumentException(readOptionalString(), readException()), this.in);
                case 13:
                    final int subclass = readVInt();
                    final String file = readOptionalString();
                    final String other = readOptionalString();
                    final String reason = readOptionalString();
                    readOptionalString(); // skip the msg - it's composed from file, other and reason
                    final Exception exception;
                    switch (subclass) {
                        case 0:
                            exception = new NoSuchFileException(file, other, reason);
                            break;
                        case 1:
                            exception = new NotDirectoryException(file);
                            break;
                        case 2:
                            exception = new DirectoryNotEmptyException(file);
                            break;
                        case 3:
                            exception = new AtomicMoveNotSupportedException(file, other, reason);
                            break;
                        case 4:
                            exception = new FileAlreadyExistsException(file, other, reason);
                            break;
                        case 5:
                            exception = new AccessDeniedException(file, other, reason);
                            break;
                        case 6:
                            exception = new FileSystemLoopException(file);
                            break;
                        case 7:
                            exception = new FileSystemException(file, other, reason);
                            break;
                        default:
                            throw new IllegalStateException("unknown FileSystemException with index " + subclass);
                    }
                    return (T) readStackTrace(exception, this.in);
                case 14:
                    return (T) readStackTrace(new IllegalStateException(readOptionalString(), readException()), this.in);
                case 16:
                    return (T) readStackTrace(new InterruptedException(readOptionalString()), this.in);
                case 17:
                    return (T) readStackTrace(new IOException(readOptionalString(), readException()), this.in);
                default:
                    throw new IOException("no such exception for id: " + key);
            }
        }
        return null;
    }

    public short readShort() throws IOException {
        return (short) (((this.in.readRawByte() & 0xFF) << 8) | (this.in.readRawByte() & 0xFF));
    }

    private static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

    public double[] readDoubleArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_DOUBLE_ARRAY;
        }
        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = this.in.readDouble();
        }
        return values;
    }

}
