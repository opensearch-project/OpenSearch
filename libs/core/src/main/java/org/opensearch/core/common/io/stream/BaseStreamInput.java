/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.core.common.io.stream;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.opensearch.Version;
import org.opensearch.common.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Locale;
import java.util.function.IntFunction;

/**
 * Foundation class for reading core types off the transport stream
 *
 * todo: refactor {@code StreamInput} primitive readers to this class
 *
 * @opensearch.internal
 */
public abstract class BaseStreamInput extends InputStream {
    // Maximum char-count to de-serialize via the thread-local CharsRef buffer
    private static final int SMALL_STRING_LIMIT = 1024;
    // Reusable bytes for deserializing strings
    private static final ThreadLocal<byte[]> stringReadBuffer = ThreadLocal.withInitial(() -> new byte[1024]);
    // Thread-local buffer for smaller strings
    private static final ThreadLocal<CharsRef> smallSpare = ThreadLocal.withInitial(() -> new CharsRef(SMALL_STRING_LIMIT));
    private Version version = Version.CURRENT;
    // Larger buffer used for long strings that can't fit into the thread-local buffer
    // We don't use a CharsRefBuilder since we exactly know the size of the character array up front
    // this prevents calling grow for every character since we don't need this
    private CharsRef largeSpare;

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
     * Closes the stream to further operations.
     */
    @Override
    public abstract void close() throws IOException;

    @Override
    public abstract int available() throws IOException;

    /**
     * This method throws an {@link EOFException} if the given number of bytes can not be read from the this stream. This method might
     * be a no-op depending on the underlying implementation if the information of the remaining bytes is not present.
     */
    protected abstract void ensureCanReadBytes(int length) throws EOFException;

    /**
     * Reads and returns a single byte.
     */
    public abstract byte readByte() throws IOException;

    /**
     * Reads a specified number of bytes into an array at the specified offset.
     *
     * @param b      the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len    the number of bytes to read
     */
    public abstract void readBytes(byte[] b, int offset, int len) throws IOException;

    public void readFully(byte[] b) throws IOException {
        readBytes(b, 0, b.length);
    }

    protected boolean readBoolean(final byte value) {
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
     * Reads a boolean.
     */
    public final boolean readBoolean() throws IOException {
        return readBoolean(readByte());
    }

    @Nullable
    public final Boolean readOptionalBoolean() throws IOException {
        final byte value = readByte();
        if (value == 2) {
            return null;
        } else {
            return readBoolean(value);
        }
    }

    /**
     * Reads four bytes and returns an int.
     */
    public int readInt() throws IOException {
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16) | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
    }

    /**
     * Reads an int stored in variable-length format.  Reads between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers
     * will always use all 5 bytes and are therefore better serialized
     * using {@link #readInt}
     */
    public int readVInt() throws IOException {
        byte b = readByte();
        int i = b & 0x7F;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        if ((b & 0x80) != 0) {
            throw new IOException("Invalid vInt ((" + Integer.toHexString(b) + " & 0x7f) << 28) | " + Integer.toHexString(i));
        }
        return i | ((b & 0x7F) << 28);
    }

    @Nullable
    public Integer readOptionalVInt() throws IOException {
        if (readBoolean()) {
            return readVInt();
        }
        return null;
    }

    public String readString() throws IOException {
        final int charCount = readArraySize();
        final CharsRef charsRef;
        if (charCount > SMALL_STRING_LIMIT) {
            if (largeSpare == null) {
                largeSpare = new CharsRef(ArrayUtil.oversize(charCount, Character.BYTES));
            } else if (largeSpare.chars.length < charCount) {
                // we don't use ArrayUtils.grow since there is no need to copy the array
                largeSpare.chars = new char[ArrayUtil.oversize(charCount, Character.BYTES)];
            }
            charsRef = largeSpare;
        } else {
            charsRef = smallSpare.get();
        }
        charsRef.length = charCount;
        int charsOffset = 0;
        int offsetByteArray = 0;
        int sizeByteArray = 0;
        int missingFromPartial = 0;
        final byte[] byteBuffer = stringReadBuffer.get();
        final char[] charBuffer = charsRef.chars;
        for (; charsOffset < charCount;) {
            final int charsLeft = charCount - charsOffset;
            int bufferFree = byteBuffer.length - sizeByteArray;
            // Determine the minimum amount of bytes that are left in the string
            final int minRemainingBytes;
            if (missingFromPartial > 0) {
                // One byte for each remaining char except for the already partially read char
                minRemainingBytes = missingFromPartial + charsLeft - 1;
                missingFromPartial = 0;
            } else {
                // Each char has at least a single byte
                minRemainingBytes = charsLeft;
            }
            final int toRead;
            if (bufferFree < minRemainingBytes) {
                // We don't have enough space left in the byte array to read as much as we'd like to so we free up as many bytes in the
                // buffer by moving unused bytes that didn't make up a full char in the last iteration to the beginning of the buffer,
                // if there are any
                if (offsetByteArray > 0) {
                    sizeByteArray = sizeByteArray - offsetByteArray;
                    switch (sizeByteArray) { // We only have 0, 1 or 2 => no need to bother with a native call to System#arrayCopy
                        case 1:
                            byteBuffer[0] = byteBuffer[offsetByteArray];
                            break;
                        case 2:
                            byteBuffer[0] = byteBuffer[offsetByteArray];
                            byteBuffer[1] = byteBuffer[offsetByteArray + 1];
                            break;
                    }
                    assert sizeByteArray <= 2 : "We never copy more than 2 bytes here since a char is 3 bytes max";
                    toRead = Math.min(bufferFree + offsetByteArray, minRemainingBytes);
                    offsetByteArray = 0;
                } else {
                    toRead = bufferFree;
                }
            } else {
                toRead = minRemainingBytes;
            }
            readBytes(byteBuffer, sizeByteArray, toRead);
            sizeByteArray += toRead;
            // As long as we at least have three bytes buffered we don't need to do any bounds checking when getting the next char since we
            // read 3 bytes per char/iteration at most
            for (; offsetByteArray < sizeByteArray - 2; offsetByteArray++) {
                final int c = byteBuffer[offsetByteArray] & 0xff;
                switch (c >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        charBuffer[charsOffset++] = (char) c;
                        break;
                    case 12:
                    case 13:
                        charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[++offsetByteArray] & 0x3F);
                        break;
                    case 14:
                        charBuffer[charsOffset++] = (char) ((c & 0x0F) << 12 | (byteBuffer[++offsetByteArray] & 0x3F) << 6
                            | (byteBuffer[++offsetByteArray] & 0x3F));
                        break;
                    default:
                        BaseStreamInput.throwOnBrokenChar(c);
                }
            }
            // try to extract chars from remaining bytes with bounds checks for multi-byte chars
            final int bufferedBytesRemaining = sizeByteArray - offsetByteArray;
            for (int i = 0; i < bufferedBytesRemaining; i++) {
                final int c = byteBuffer[offsetByteArray] & 0xff;
                switch (c >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        charBuffer[charsOffset++] = (char) c;
                        offsetByteArray++;
                        break;
                    case 12:
                    case 13:
                        missingFromPartial = 2 - (bufferedBytesRemaining - i);
                        if (missingFromPartial == 0) {
                            offsetByteArray++;
                            charBuffer[charsOffset++] = (char) ((c & 0x1F) << 6 | byteBuffer[offsetByteArray++] & 0x3F);
                        }
                        ++i;
                        break;
                    case 14:
                        missingFromPartial = 3 - (bufferedBytesRemaining - i);
                        ++i;
                        break;
                    default:
                        BaseStreamInput.throwOnBrokenChar(c);
                }
            }
        }
        return charsRef.toString();
    }

    @Nullable
    public String readOptionalString() throws IOException {
        if (readBoolean()) {
            return readString();
        }
        return null;
    }

    private static void throwOnBrokenChar(int c) throws IOException {
        throw new IOException("Invalid string; unexpected character: " + c + " hex: " + Integer.toHexString(c));
    }

    /**
     * Reads a vint via {@link #readVInt()} and applies basic checks to ensure the read array size is sane.
     * This method uses {@link #ensureCanReadBytes(int)} to ensure this stream has enough bytes to read for the read array size.
     */
    protected int readArraySize() throws IOException {
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
        ensureCanReadBytes(arraySize);
        return arraySize;
    }

    /**
     * Reads a collection of objects
     */
    @SuppressWarnings("unchecked")
    protected <S extends BaseStreamInput, T, C extends Collection<? super T>> C readCollection(
        BaseWriteable.Reader<S, T> reader,
        IntFunction<C> constructor,
        C empty
    ) throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return empty;
        }
        C builder = constructor.apply(count);
        for (int i = 0; i < count; i++) {
            builder.add(reader.read((S) this));
        }
        return builder;
    }
}
