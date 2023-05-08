/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common.io.stream;

import com.google.protobuf.CodedOutputStream;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufWriteable.Writer;
import org.opensearch.common.unit.TimeValue;

/**
 * A class for additional methods to write to a {@link CodedOutputStream}.
 */
public class ProtobufStreamOutput {

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
        final ProtobufWriteable.Writer<V> valueWriter,
        CodedOutputStream out
    ) throws IOException {
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            keyWriter.write(out, entry.getKey());
            valueWriter.write(out, entry.getValue());
        }
    }

    public void writeOptionalWriteable(@Nullable ProtobufWriteable writeable, CodedOutputStream out) throws IOException {
        if (writeable != null) {
            out.writeBoolNoTag(true);
            writeable.writeTo(out);
        } else {
            out.writeBoolNoTag(false);
        }
    }

    /**
     * Write a {@link TimeValue} to the stream
     */
    public void writeTimeValue(TimeValue timeValue, CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(timeValue.duration());
        out.writeRawByte((byte) timeValue.timeUnit().ordinal());
    }

    /**
     * Writes an EnumSet with type E that by serialized it based on it's ordinal value
     */
    public <E extends Enum<E>> void writeEnumSet(EnumSet<E> enumSet, CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(enumSet.size());
        for (E e : enumSet) {
            out.writeEnumNoTag(e.ordinal());
        }
    }

    public void writeOptionalLong(@Nullable Long l, CodedOutputStream out) throws IOException {
        if (l == null) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            out.writeInt64NoTag(l);
        }
    }

    public void writeOptionalString(@Nullable String str, CodedOutputStream out) throws IOException {
        if (str == null) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            out.writeStringNoTag(str);
        }
    }

    public void writeOptionalBoolean(@Nullable Boolean b, CodedOutputStream out) throws IOException {
        byte two = 2;
        if (b == null) {
            out.write(two);
        } else {
            out.writeBoolNoTag(b);
        }
    }

    /**
     * Writes a collection of objects via a {@link Writer}.
     *
     * @param collection the collection of objects
     * @throws IOException if an I/O exception occurs writing the collection
     */
    public <T> void writeCollection(final Collection<T> collection, final Writer<T> writer, CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(collection.size());
        for (final T val : collection) {
            writer.write(out, val);
        }
    }

    public void writeStringArray(String[] array, CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(array.length);
        for (String s : array) {
            out.writeStringNoTag(s);
        }
    }

    /**
     * Writes a {@link ProtobufNamedWriteable} to the current stream, by first writing its name and then the object itself
     */
    public void writeNamedWriteable(ProtobufNamedWriteable namedWriteable, CodedOutputStream out) throws IOException {
        out.writeStringNoTag(namedWriteable.getWriteableName());
        namedWriteable.writeTo(out);
    }

    /**
     * Writes a string array, for nullable string, writes it as 0 (empty string).
     */
    public void writeStringArrayNullable(@Nullable String[] array, CodedOutputStream out) throws IOException {
        if (array == null) {
            out.writeInt32NoTag(0);
        } else {
            writeStringArray(array, out);
        }
    }

    /**
     * Write an optional {@link TimeValue} to the stream.
     */
    public void writeOptionalTimeValue(@Nullable TimeValue timeValue, CodedOutputStream out) throws IOException {
        if (timeValue == null) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            writeTimeValue(timeValue, out);
        }
    }

    /**
     * Same as {@link #writeArray(Writer, Object[], CodedOutputStream)} but the provided array may be null. An additional boolean value is
     * serialized to indicate whether the array was null or not.
     */
    public <T> void writeOptionalArray(final Writer<T> writer, final @Nullable T[] array, CodedOutputStream out) throws IOException {
        if (array == null) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            writeArray(writer, array, out);
        }
    }

    public <T> void writeArray(final Writer<T> writer, final T[] array, CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(array.length);
        for (T value : array) {
            writer.write(out, value);
        }
    }

}
