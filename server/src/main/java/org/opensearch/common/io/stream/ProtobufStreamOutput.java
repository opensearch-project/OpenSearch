/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common.io.stream;

import com.google.protobuf.CodedOutputStream;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.ProtobufOpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufWriteable.Writer;
import org.opensearch.common.unit.TimeValue;

/**
 * A class for additional methods to write to a {@link CodedOutputStream}.
 */
public class ProtobufStreamOutput {

    private static final int MAX_NESTED_EXCEPTION_LEVEL = 100;
    private CodedOutputStream out;
    private Version version = Version.CURRENT;
    private Set<String> features = Collections.emptySet();

    public ProtobufStreamOutput(CodedOutputStream out) {
        this.out = out;
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

    /**
     * Write a {@link Map} of {@code K}-type keys to {@code V}-type.
     * <pre><code>
     * Map&lt;String, String&gt; map = ...;
     * out.writeMap(map, StreamOutput::writeString, StreamOutput::writeString);
     * </code></pre>
     *
     * @param keyWriter The key writer
     * @param valueWriter The value writer
     */
    public final <K, V> void writeMap(
        final Map<K, V> map,
        final ProtobufWriteable.Writer<K> keyWriter,
        final ProtobufWriteable.Writer<V> valueWriter
    ) throws IOException {
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            keyWriter.write(this.out, entry.getKey());
            valueWriter.write(this.out, entry.getValue());
        }
    }

    public void writeOptionalWriteable(@Nullable ProtobufWriteable writeable) throws IOException {
        if (writeable != null) {
            this.out.writeBoolNoTag(true);
            writeable.writeTo(this.out);
        } else {
            this.out.writeBoolNoTag(false);
        }
    }

    /**
     * Write a {@link TimeValue} to the stream
     */
    public void writeTimeValue(TimeValue timeValue) throws IOException {
        this.out.writeInt64NoTag(timeValue.duration());
        this.out.writeRawByte((byte) timeValue.timeUnit().ordinal());
    }

    /**
     * Writes an EnumSet with type E that by serialized it based on it's ordinal value
     */
    public <E extends Enum<E>> void writeEnumSet(EnumSet<E> enumSet) throws IOException {
        this.out.writeInt32NoTag(enumSet.size());
        for (E e : enumSet) {
            this.out.writeEnumNoTag(e.ordinal());
        }
    }

    public void writeOptionalLong(@Nullable Long l) throws IOException {
        if (l == null) {
            this.out.writeBoolNoTag(false);
        } else {
            this.out.writeBoolNoTag(true);
            this.out.writeInt64NoTag(l);
        }
    }

    public void writeOptionalString(@Nullable String str) throws IOException {
        if (str == null) {
            this.out.writeBoolNoTag(false);
        } else {
            this.out.writeBoolNoTag(true);
            this.out.writeStringNoTag(str);
        }
    }

    public void writeOptionalBoolean(@Nullable Boolean b) throws IOException {
        byte two = 2;
        if (b == null) {
            this.out.write(two);
        } else {
            this.out.writeBoolNoTag(b);
        }
    }

    /**
     * Writes a collection of a strings. The corresponding collection can be read from a stream input using
     * {@link ProtobufStreamInput#readList(ProtobufWriteable.Reader)}.
     *
     * @param collection the collection of strings
     * @throws IOException if an I/O exception occurs writing the collection
     */
    public void writeStringCollection(final Collection<String> collection) throws IOException {
        writeCollection(collection, CodedOutputStream::writeStringNoTag);
    }

    /**
     * Writes a collection of objects via a {@link Writer}.
     *
     * @param collection the collection of objects
     * @throws IOException if an I/O exception occurs writing the collection
     */
    public <T> void writeCollection(final Collection<T> collection, final Writer<T> writer) throws IOException {
        this.out.writeInt32NoTag(collection.size());
        for (final T val : collection) {
            writer.write(this.out, val);
        }
    }

    public void writeStringArray(String[] array) throws IOException {
        this.out.writeInt32NoTag(array.length);
        for (String s : array) {
            this.out.writeStringNoTag(s);
        }
    }

    /**
     * Writes a {@link ProtobufNamedWriteable} to the current stream, by first writing its name and then the object itself
     */
    public void writeNamedWriteable(ProtobufNamedWriteable namedWriteable) throws IOException {
        this.out.writeStringNoTag(namedWriteable.getWriteableName());
        namedWriteable.writeTo(this.out);
    }

    /**
     * Writes a string array, for nullable string, writes it as 0 (empty string).
     */
    public void writeStringArrayNullable(@Nullable String[] array) throws IOException {
        if (array == null) {
            this.out.writeInt32NoTag(0);
        } else {
            writeStringArray(array);
        }
    }

    /**
     * Write an optional {@link TimeValue} to the stream.
     */
    public void writeOptionalTimeValue(@Nullable TimeValue timeValue) throws IOException {
        if (timeValue == null) {
            this.out.writeBoolNoTag(false);
        } else {
            this.out.writeBoolNoTag(true);
            writeTimeValue(timeValue);
        }
    }

    /**
     * Same as {@link #writeArray(Writer, Object[])} but the provided array may be null. An additional boolean value is
     * serialized to indicate whether the array was null or not.
     */
    public <T> void writeOptionalArray(final Writer<T> writer, final @Nullable T[] array) throws IOException {
        if (array == null) {
            this.out.writeBoolNoTag(false);
        } else {
            this.out.writeBoolNoTag(true);
            writeArray(writer, array);
        }
    }

    public <T> void writeArray(final Writer<T> writer, final T[] array) throws IOException {
        this.out.writeInt32NoTag(array.length);
        for (T value : array) {
            writer.write(this.out, value);
        }
    }

    /**
    * Set the features on the stream. See {@link StreamOutput#hasFeature(String)}.
    *
    * @param features the features on the stream
    */
    public void setFeatures(final Set<String> features) {
        assert this.features.isEmpty() : this.features;
        this.features = Collections.unmodifiableSet(new HashSet<>(features));
    }

    public Set<String> getFeatures() {
        return this.features;
    }

    public void writeException(Throwable throwable) throws IOException {
        writeException(throwable, throwable, 0);
    }

    private void writeException(Throwable rootException, Throwable throwable, int nestedLevel) throws IOException {
        if (throwable == null) {
            this.out.writeBoolNoTag(false);
        } else if (nestedLevel > MAX_NESTED_EXCEPTION_LEVEL) {
            assert failOnTooManyNestedExceptions(rootException);
            writeException(new IllegalStateException("too many nested exceptions"));
        } else {
            this.out.writeBoolNoTag(true);
            boolean writeCause = true;
            boolean writeMessage = true;
            if (throwable instanceof NullPointerException) {
                this.out.writeInt32NoTag(4);
                writeCause = false;
            } else if (throwable instanceof NumberFormatException) {
                this.out.writeInt32NoTag(5);
                writeCause = false;
            } else if (throwable instanceof IllegalArgumentException) {
                this.out.writeInt32NoTag(6);
            } else if (throwable instanceof AlreadyClosedException) {
                this.out.writeInt32NoTag(7);
            } else if (throwable instanceof EOFException) {
                this.out.writeInt32NoTag(8);
                writeCause = false;
            } else if (throwable instanceof SecurityException) {
                this.out.writeInt32NoTag(9);
            } else if (throwable instanceof StringIndexOutOfBoundsException) {
                this.out.writeInt32NoTag(10);
                writeCause = false;
            } else if (throwable instanceof ArrayIndexOutOfBoundsException) {
                this.out.writeInt32NoTag(11);
                writeCause = false;
            } else if (throwable instanceof FileNotFoundException) {
                this.out.writeInt32NoTag(12);
                writeCause = false;
            } else if (throwable instanceof FileSystemException) {
                this.out.writeInt32NoTag(13);
                if (throwable instanceof NoSuchFileException) {
                    this.out.writeInt32NoTag(0);
                } else if (throwable instanceof NotDirectoryException) {
                    this.out.writeInt32NoTag(1);
                } else if (throwable instanceof DirectoryNotEmptyException) {
                    this.out.writeInt32NoTag(2);
                } else if (throwable instanceof AtomicMoveNotSupportedException) {
                    this.out.writeInt32NoTag(3);
                } else if (throwable instanceof FileAlreadyExistsException) {
                    this.out.writeInt32NoTag(4);
                } else if (throwable instanceof AccessDeniedException) {
                    this.out.writeInt32NoTag(5);
                } else if (throwable instanceof FileSystemLoopException) {
                    this.out.writeInt32NoTag(6);
                } else {
                    this.out.writeInt32NoTag(7);
                }
                writeOptionalString(((FileSystemException) throwable).getFile());
                writeOptionalString(((FileSystemException) throwable).getOtherFile());
                writeOptionalString(((FileSystemException) throwable).getReason());
                writeCause = false;
            } else if (throwable instanceof IllegalStateException) {
                this.out.writeInt32NoTag(14);
            } else if (throwable instanceof LockObtainFailedException) {
                this.out.writeInt32NoTag(15);
            } else if (throwable instanceof InterruptedException) {
                this.out.writeInt32NoTag(16);
                writeCause = false;
            } else if (throwable instanceof IOException) {
                this.out.writeInt32NoTag(17);
            } else {
                final ProtobufOpenSearchException ex;
                if (throwable instanceof ProtobufOpenSearchException
                    && ProtobufOpenSearchException.isRegistered(throwable.getClass(), version)) {
                    ex = (ProtobufOpenSearchException) throwable;
                } else {
                    ex = new ProtobufNotSerializableExceptionWrapper(throwable);
                }
                this.out.writeInt32NoTag(0);
                this.out.writeInt32NoTag(ProtobufOpenSearchException.getId(ex.getClass()));
                ex.writeTo(this.out);
                return;
            }
            if (writeMessage) {
                writeOptionalString(throwable.getMessage());
            }
            if (writeCause) {
                writeException(rootException, throwable.getCause(), nestedLevel + 1);
            }
            ProtobufOpenSearchException.writeStackTraces(throwable, this.out, new ProtobufStreamOutput(this.out));
        }
    }

    boolean failOnTooManyNestedExceptions(Throwable throwable) {
        throw new AssertionError("too many nested exceptions", throwable);
    }

    /**
     * Writes an enum with type E based on its ordinal value
     */
    public <E extends Enum<E>> void writeEnum(E enumValue) throws IOException {
        this.out.writeInt32NoTag(enumValue.ordinal());
    }

    private static final ThreadLocal<byte[]> scratch = ThreadLocal.withInitial(() -> new byte[1024]);

    public final void writeShort(short v) throws IOException {
        final byte[] buffer = scratch.get();
        buffer[0] = (byte) (v >> 8);
        buffer[1] = (byte) v;
        this.out.writeByteArrayNoTag(buffer);
    }

    public void writeDoubleArray(double[] values) throws IOException {
        this.out.writeInt32NoTag(values.length);
        for (double value : values) {
            this.out.writeDoubleNoTag(value);
        }
    }

}
