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

package org.opensearch.common.io.stream;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Build;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.CharArrays;
import org.opensearch.common.Nullable;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.SecureString;
import org.opensearch.common.text.Text;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.BaseStreamInput;
import org.opensearch.core.common.io.stream.BaseWriteable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.opensearch.OpenSearchException.readStackTrace;

/**
 * A stream from this node to another node. Technically, it can also be streamed to a byte array but that is mostly for testing.
 *
 * This class's methods are optimized so you can put the methods that read and write a class next to each other and you can scan them
 * visually for differences. That means that most variables should be read and written in a single line so even large objects fit both
 * reading and writing on the screen. It also means that the methods on this class are named very similarly to {@link StreamOutput}. Finally
 * it means that the "barrier to entry" for adding new methods to this class is relatively low even though it is a shared class with code
 * everywhere. That being said, this class deals primarily with {@code List}s rather than Arrays. For the most part calls should adapt to
 * lists, either by storing {@code List}s internally or just converting to and from a {@code List} when calling. This comment is repeated
 * on {@link StreamInput}.
 *
 * @opensearch.internal
 */
public abstract class StreamInput extends BaseStreamInput {

    /**
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference() throws IOException {
        int length = readArraySize();
        return readBytesReference(length);
    }

    /**
     * Reads an optional bytes reference from this stream. It might hold an actual reference to the underlying bytes of the stream. Use this
     * only if you must differentiate null from empty. Use {@link StreamInput#readBytesReference()} and
     * {@link StreamOutput#writeBytesReference(BytesReference)} if you do not.
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
    public BytesReference readBytesReference(int length) throws IOException {
        if (length == 0) {
            return BytesArray.EMPTY;
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return new BytesArray(bytes, 0, length);
    }

    public BytesRef readBytesRef() throws IOException {
        int length = readArraySize();
        return readBytesRef(length);
    }

    public BytesRef readBytesRef(int length) throws IOException {
        if (length == 0) {
            return new BytesRef();
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return new BytesRef(bytes, 0, length);
    }

    public short readShort() throws IOException {
        return (short) (((readByte() & 0xFF) << 8) | (readByte() & 0xFF));
    }

    /**
     * Reads an optional {@link Integer}.
     */
    public Integer readOptionalInt() throws IOException {
        if (readBoolean()) {
            return readInt();
        }
        return null;
    }

    /**
     * Reads eight bytes and returns a long.
     */
    public long readLong() throws IOException {
        return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
    }

    /**
     * Reads a long stored in variable-length format. Reads between one and ten bytes. Smaller values take fewer bytes. Negative numbers
     * are encoded in ten bytes so prefer {@link #readLong()} or {@link #readZLong()} for negative numbers.
     */
    public long readVLong() throws IOException {
        byte b = readByte();
        long i = b & 0x7FL;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= ((b & 0x7FL) << 56);
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        if (b != 0 && b != 1) {
            throw new IOException("Invalid vlong (" + Integer.toHexString(b) + " << 63) | " + Long.toHexString(i));
        }
        i |= ((long) b) << 63;
        return i;
    }

    @Nullable
    public Long readOptionalVLong() throws IOException {
        if (readBoolean()) {
            return readVLong();
        }
        return null;
    }

    public long readZLong() throws IOException {
        long accumulator = 0L;
        int i = 0;
        long currentByte;
        while (((currentByte = readByte()) & 0x80L) != 0) {
            accumulator |= (currentByte & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IOException("variable-length stream is too long");
            }
        }
        return BitUtil.zigZagDecode(accumulator | (currentByte << i));
    }

    @Nullable
    public Long readOptionalLong() throws IOException {
        if (readBoolean()) {
            return readLong();
        }
        return null;
    }

    public BigInteger readBigInteger() throws IOException {
        return new BigInteger(readString());
    }

    @Nullable
    public Text readOptionalText() throws IOException {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        return new Text(readBytesReference(length));
    }

    public Text readText() throws IOException {
        // use StringAndBytes so we can cache the string if its ever converted to it
        int length = readInt();
        return new Text(readBytesReference(length));
    }

    @Nullable
    public SecureString readOptionalSecureString() throws IOException {
        SecureString value = null;
        BytesReference bytesRef = readOptionalBytesReference();
        if (bytesRef != null) {
            byte[] bytes = BytesReference.toBytes(bytesRef);
            try {
                value = new SecureString(CharArrays.utf8BytesToChars(bytes));
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }
        return value;
    }

    @Nullable
    public Float readOptionalFloat() throws IOException {
        if (readBoolean()) {
            return readFloat();
        }
        return null;
    }

    public SecureString readSecureString() throws IOException {
        BytesReference bytesRef = readBytesReference();
        byte[] bytes = BytesReference.toBytes(bytesRef);
        try {
            return new SecureString(CharArrays.utf8BytesToChars(bytes));
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Nullable
    public final Double readOptionalDouble() throws IOException {
        if (readBoolean()) {
            return readDouble();
        }
        return null;
    }

    public String[] readStringArray() throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = readString();
        }
        return ret;
    }

    @Nullable
    public String[] readOptionalStringArray() throws IOException {
        if (readBoolean()) {
            return readStringArray();
        }
        return null;
    }

    /**
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    public <K, V> Map<K, V> readMap(Writeable.Reader<K> keyReader, Writeable.Reader<V> valueReader) throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<K, V> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            K key = keyReader.read(this);
            V value = valueReader.read(this);
            map.put(key, value);
        }
        return map;
    }

    /**
     * Read a {@link Map} of {@code K}-type keys to {@code V}-type {@link List}s.
     * <pre><code>
     * Map&lt;String, List&lt;String&gt;&gt; map = in.readMapOfLists(StreamInput::readString, StreamInput::readString);
     * </code></pre>
     * If the map or a list in it contains any elements it will be mutable, otherwise either the empty map or empty lists it contains
     * might be immutable.
     *
     * @param keyReader The key reader
     * @param valueReader The value reader
     * @return Never {@code null}.
     */
    public <K, V> Map<K, List<V>> readMapOfLists(final Writeable.Reader<K> keyReader, final Writeable.Reader<V> valueReader)
        throws IOException {
        final int size = readArraySize();
        if (size == 0) {
            return Collections.emptyMap();
        }
        final Map<K, List<V>> map = new HashMap<>(size);
        for (int i = 0; i < size; ++i) {
            map.put(keyReader.read(this), readList(valueReader));
        }
        return map;
    }

    /**
     * If the returned map contains any entries it will be mutable. If it is empty it might be immutable.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public Map<String, Object> readMap() throws IOException {
        return (Map<String, Object>) readGenericValue();
    }

    /**
     * Reads a value of unspecified type. If a collection is read then the collection will be mutable if it contains any entry but might
     * be immutable if it is empty.
     */
    @Nullable
    public Object readGenericValue() throws IOException {
        byte type = readByte();
        BaseWriteable.Reader<StreamInput, ?> r = BaseWriteable.WriteableRegistry.getReader(type);
        if (r != null) {
            return r.read(this);
        }

        switch (type) {
            case -1:
                return null;
            case 0:
                return readString();
            case 1:
                return readInt();
            case 2:
                return readLong();
            case 3:
                return readFloat();
            case 4:
                return readDouble();
            case 5:
                return readBoolean();
            case 6:
                return readByteArray();
            case 7:
                return readArrayList();
            case 8:
                return readArray();
            case 9:
                return readLinkedHashMap();
            case 10:
                return readHashMap();
            case 11:
                return readByte();
            case 12:
                return readDate();
            case 14:
                return readBytesReference();
            case 15:
                return readText();
            case 16:
                return readShort();
            case 17:
                return readIntArray();
            case 18:
                return readLongArray();
            case 19:
                return readFloatArray();
            case 20:
                return readDoubleArray();
            case 21:
                return readBytesRef();
            case 23:
                return readZonedDateTime();
            case 24:
                return readCollection(StreamInput::readGenericValue, LinkedHashSet::new, Collections.emptySet());
            case 25:
                return readCollection(StreamInput::readGenericValue, HashSet::new, Collections.emptySet());
            case 26:
                return readBigInteger();
            default:
                throw new IOException("Can't read unknown type [" + type + "]");
        }
    }

    /**
     * Read an {@link Instant} from the stream with nanosecond resolution
     */
    public final Instant readInstant() throws IOException {
        return Instant.ofEpochSecond(readLong(), readInt());
    }

    /**
     * Read an optional {@link Instant} from the stream. Returns <code>null</code> when
     * no instant is present.
     */
    @Nullable
    public final Instant readOptionalInstant() throws IOException {
        final boolean present = readBoolean();
        return present ? readInstant() : null;
    }

    @SuppressWarnings("unchecked")
    private List readArrayList() throws IOException {
        int size = readArraySize();
        if (size == 0) {
            return Collections.emptyList();
        }
        List list = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            list.add(readGenericValue());
        }
        return list;
    }

    private ZonedDateTime readZonedDateTime() throws IOException {
        final String timeZoneId = readString();
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(readLong()), ZoneId.of(timeZoneId));
    }

    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private Object[] readArray() throws IOException {
        int size8 = readArraySize();
        if (size8 == 0) {
            return EMPTY_OBJECT_ARRAY;
        }
        Object[] list8 = new Object[size8];
        for (int i = 0; i < size8; i++) {
            list8[i] = readGenericValue();
        }
        return list8;
    }

    private Map readLinkedHashMap() throws IOException {
        int size9 = readArraySize();
        if (size9 == 0) {
            return Collections.emptyMap();
        }
        Map map9 = new LinkedHashMap(size9);
        for (int i = 0; i < size9; i++) {
            map9.put(readString(), readGenericValue());
        }
        return map9;
    }

    private Map readHashMap() throws IOException {
        int size10 = readArraySize();
        if (size10 == 0) {
            return Collections.emptyMap();
        }
        Map map10 = new HashMap(size10);
        for (int i = 0; i < size10; i++) {
            map10.put(readString(), readGenericValue());
        }
        return map10;
    }

    private Date readDate() throws IOException {
        return new Date(readLong());
    }

    /**
     * Read a {@linkplain ZoneId}.
     */
    public ZoneId readZoneId() throws IOException {
        return ZoneId.of(readString());
    }

    /**
     * Read an optional {@linkplain ZoneId}.
     */
    public ZoneId readOptionalZoneId() throws IOException {
        if (readBoolean()) {
            return ZoneId.of(readString());
        }
        return null;
    }

    private static final int[] EMPTY_INT_ARRAY = new int[0];

    public int[] readIntArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_INT_ARRAY;
        }
        int[] values = new int[length];
        for (int i = 0; i < length; i++) {
            values[i] = readInt();
        }
        return values;
    }

    public int[] readVIntArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_INT_ARRAY;
        }
        int[] values = new int[length];
        for (int i = 0; i < length; i++) {
            values[i] = readVInt();
        }
        return values;
    }

    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    public long[] readLongArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_LONG_ARRAY;
        }
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = readLong();
        }
        return values;
    }

    public long[] readVLongArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_LONG_ARRAY;
        }
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = readVLong();
        }
        return values;
    }

    private static final float[] EMPTY_FLOAT_ARRAY = new float[0];

    public float[] readFloatArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_FLOAT_ARRAY;
        }
        float[] values = new float[length];
        for (int i = 0; i < length; i++) {
            values[i] = readFloat();
        }
        return values;
    }

    private static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

    public double[] readDoubleArray() throws IOException {
        int length = readArraySize();
        if (length == 0) {
            return EMPTY_DOUBLE_ARRAY;
        }
        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = readDouble();
        }
        return values;
    }

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public byte[] readByteArray() throws IOException {
        final int length = readArraySize();
        if (length == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        final byte[] bytes = new byte[length];
        readBytes(bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Reads an array from the stream using the specified {@link org.opensearch.common.io.stream.Writeable.Reader} to read array elements
     * from the stream. This method can be seen as the reader version of {@link StreamOutput#writeArray(Writeable.Writer, Object[])}. It is
     * assumed that the stream first contains a variable-length integer representing the size of the array, and then contains that many
     * elements that can be read from the stream.
     *
     * @param reader        the reader used to read individual elements
     * @param arraySupplier a supplier used to construct a new array
     * @param <T>           the type of the elements of the array
     * @return an array read from the stream
     * @throws IOException if an I/O exception occurs while reading the array
     */
    public <T> T[] readArray(final Writeable.Reader<T> reader, final IntFunction<T[]> arraySupplier) throws IOException {
        final int length = readArraySize();
        final T[] values = arraySupplier.apply(length);
        for (int i = 0; i < length; i++) {
            values[i] = reader.read(this);
        }
        return values;
    }

    public <T> T[] readOptionalArray(Writeable.Reader<T> reader, IntFunction<T[]> arraySupplier) throws IOException {
        return readBoolean() ? readArray(reader, arraySupplier) : null;
    }

    @Nullable
    public <T extends Writeable> T readOptionalWriteable(Writeable.Reader<T> reader) throws IOException {
        if (readBoolean()) {
            T t = reader.read(this);
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

    @Nullable
    @SuppressWarnings("unchecked")
    public <T extends Exception> T readException() throws IOException {
        if (readBoolean()) {
            int key = readVInt();
            switch (key) {
                case 0:
                    final int ord = readVInt();
                    return (T) OpenSearchException.readException(this, ord);
                case 1:
                    String msg1 = readOptionalString();
                    String resource1 = readOptionalString();
                    return (T) readStackTrace(new CorruptIndexException(msg1, resource1, readException()), this);
                case 2:
                    String resource2 = readOptionalString();
                    int version2 = readInt();
                    int minVersion2 = readInt();
                    int maxVersion2 = readInt();
                    return (T) readStackTrace(new IndexFormatTooNewException(resource2, version2, minVersion2, maxVersion2), this);
                case 3:
                    String resource3 = readOptionalString();
                    if (readBoolean()) {
                        int version3 = readInt();
                        int minVersion3 = readInt();
                        int maxVersion3 = readInt();
                        return (T) readStackTrace(new IndexFormatTooOldException(resource3, version3, minVersion3, maxVersion3), this);
                    } else {
                        String version3 = readOptionalString();
                        return (T) readStackTrace(new IndexFormatTooOldException(resource3, version3), this);
                    }
                case 4:
                    return (T) readStackTrace(new NullPointerException(readOptionalString()), this);
                case 5:
                    return (T) readStackTrace(new NumberFormatException(readOptionalString()), this);
                case 6:
                    return (T) readStackTrace(new IllegalArgumentException(readOptionalString(), readException()), this);
                case 7:
                    return (T) readStackTrace(new AlreadyClosedException(readOptionalString(), readException()), this);
                case 8:
                    return (T) readStackTrace(new EOFException(readOptionalString()), this);
                case 9:
                    return (T) readStackTrace(new SecurityException(readOptionalString(), readException()), this);
                case 10:
                    return (T) readStackTrace(new StringIndexOutOfBoundsException(readOptionalString()), this);
                case 11:
                    return (T) readStackTrace(new ArrayIndexOutOfBoundsException(readOptionalString()), this);
                case 12:
                    return (T) readStackTrace(new FileNotFoundException(readOptionalString()), this);
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
                    return (T) readStackTrace(exception, this);
                case 14:
                    return (T) readStackTrace(new IllegalStateException(readOptionalString(), readException()), this);
                case 15:
                    return (T) readStackTrace(new LockObtainFailedException(readOptionalString(), readException()), this);
                case 16:
                    return (T) readStackTrace(new InterruptedException(readOptionalString()), this);
                case 17:
                    return (T) readStackTrace(new IOException(readOptionalString(), readException()), this);
                case 18:
                    final boolean isExecutorShutdown = readBoolean();
                    return (T) readStackTrace(new OpenSearchRejectedExecutionException(readOptionalString(), isExecutorShutdown), this);
                default:
                    throw new IOException("no such exception for id: " + key);
            }
        }
        return null;
    }

    /** Reads the OpenSearch Version from the input stream */
    public Version readVersion() throws IOException {
        return Version.fromId(readVInt());
    }

    /** Reads the {@link Version} from the input stream */
    public Build readBuild() throws IOException {
        // the following is new for opensearch: we write the distribution to support any "forks"
        final String distribution = readString();
        // be lenient when reading on the wire, the enumeration values from other versions might be different than what we know
        final Build.Type type = Build.Type.fromDisplayName(readString(), false);
        String hash = readString();
        String date = readString();
        boolean snapshot = readBoolean();
        final String version = readString();
        return new Build(type, hash, date, snapshot, version, distribution);
    }

    /**
     * Get the registry of named writeables if this stream has one,
     * {@code null} otherwise.
     */
    public NamedWriteableRegistry namedWriteableRegistry() {
        return null;
    }

    /**
     * Reads a {@link NamedWriteable} from the current stream, by first reading its name and then looking for
     * the corresponding entry in the registry by name, so that the proper object can be read and returned.
     * Default implementation throws {@link UnsupportedOperationException} as StreamInput doesn't hold a registry.
     * Use {@link FilterInputStream} instead which wraps a stream and supports a {@link NamedWriteableRegistry} too.
     */
    @Nullable
    public <C extends NamedWriteable> C readNamedWriteable(@SuppressWarnings("unused") Class<C> categoryClass) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    /**
     * Reads a {@link NamedWriteable} from the current stream with the given name. It is assumed that the caller obtained the name
     * from other source, so it's not read from the stream. The name is used for looking for
     * the corresponding entry in the registry by name, so that the proper object can be read and returned.
     * Default implementation throws {@link UnsupportedOperationException} as StreamInput doesn't hold a registry.
     * Use {@link FilterInputStream} instead which wraps a stream and supports a {@link NamedWriteableRegistry} too.
     *
     * Prefer {@link StreamInput#readNamedWriteable(Class)} and {@link StreamOutput#writeNamedWriteable(NamedWriteable)} unless you
     * have a compelling reason to use this method instead.
     */
    @Nullable
    public <C extends NamedWriteable> C readNamedWriteable(
        @SuppressWarnings("unused") Class<C> categoryClass,
        @SuppressWarnings("unused") String name
    ) throws IOException {
        throw new UnsupportedOperationException("can't read named writeable from StreamInput");
    }

    /**
     * Reads an optional {@link NamedWriteable}.
     */
    @Nullable
    public <C extends NamedWriteable> C readOptionalNamedWriteable(Class<C> categoryClass) throws IOException {
        if (readBoolean()) {
            return readNamedWriteable(categoryClass);
        }
        return null;
    }

    /**
     * Reads a list of objects. The list is expected to have been written using {@link StreamOutput#writeList(List)}.
     * If the returned list contains any entries it will be mutable. If it is empty it might be immutable.
     *
     * @return the list of objects
     * @throws IOException if an I/O exception occurs reading the list
     */
    public <T> List<T> readList(final BaseWriteable.Reader<StreamInput, T> reader) throws IOException {
        return readCollection(reader, ArrayList::new, Collections.emptyList());
    }

    /**
     * Reads a list of strings. The list is expected to have been written using {@link StreamOutput#writeStringCollection(Collection)}.
     * If the returned list contains any entries it will be mutable. If it is empty it might be immutable.
     *
     * @return the list of strings
     * @throws IOException if an I/O exception occurs reading the list
     */
    public List<String> readStringList() throws IOException {
        return readList(StreamInput::readString);
    }

    /**
     * Reads an optional list of strings. The list is expected to have been written using
     * {@link StreamOutput#writeOptionalStringCollection(Collection)}. If the returned list contains any entries it will be mutable.
     * If it is empty it might be immutable.
     *
     * @return the list of strings
     * @throws IOException if an I/O exception occurs reading the list
     */
    public List<String> readOptionalStringList() throws IOException {
        final boolean isPresent = readBoolean();
        if (isPresent) {
            return readList(StreamInput::readString);
        } else {
            return null;
        }
    }

    /**
     * Reads a set of objects. If the returned set contains any entries it will be mutable. If it is empty it might be immutable.
     */
    public <T> Set<T> readSet(Writeable.Reader<T> reader) throws IOException {
        return readCollection(reader, HashSet::new, Collections.emptySet());
    }

    /**
     * Reads a list of {@link NamedWriteable}s. If the returned list contains any entries it will be mutable.
     * If it is empty it might be immutable.
     */
    public <T extends NamedWriteable> List<T> readNamedWriteableList(Class<T> categoryClass) throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return Collections.emptyList();
        }
        List<T> builder = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            builder.add(readNamedWriteable(categoryClass));
        }
        return builder;
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

    public static StreamInput wrap(byte[] bytes) {
        return wrap(bytes, 0, bytes.length);
    }

    public static StreamInput wrap(byte[] bytes, int offset, int length) {
        return new InputStreamStreamInput(new ByteArrayInputStream(bytes, offset, length), length);
    }

    /**
     * This method throws an {@link EOFException} if the given number of bytes can not be read from the this stream. This method might
     * be a no-op depending on the underlying implementation if the information of the remaining bytes is not present.
     */
    @Override
    protected abstract void ensureCanReadBytes(int length) throws EOFException;

    private static final TimeUnit[] TIME_UNITS = TimeUnit.values();

    static {
        // assert the exact form of the TimeUnit values to ensure we're not silently broken by a JDK change
        if (Arrays.equals(
            TIME_UNITS,
            new TimeUnit[] {
                TimeUnit.NANOSECONDS,
                TimeUnit.MICROSECONDS,
                TimeUnit.MILLISECONDS,
                TimeUnit.SECONDS,
                TimeUnit.MINUTES,
                TimeUnit.HOURS,
                TimeUnit.DAYS }
        ) == false) {
            throw new AssertionError("Incompatible JDK version used that breaks assumptions on the structure of the TimeUnit enum");
        }
    }

    /**
     * Read a {@link TimeValue} from the stream
     */
    public TimeValue readTimeValue() throws IOException {
        long duration = readZLong();
        TimeUnit timeUnit = TIME_UNITS[readByte()];
        return new TimeValue(duration, timeUnit);
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
}
