/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.IngestionShardPointer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/**
 * A pointer into object storage identifying the next unread byte within a specific object. A pointer is the pair
 * {@code (objectKey, byteOffset)}: for a single-object source the key is constant across the shard; for a prefix
 * source the key varies as the shard iterates the objects it owns in lexicographic order.
 * <p>
 * The pointer serializes to a byte encoding that is both <em>injection-safe</em> (no delimiter can be spoofed by
 * key content, so no two distinct {@code (key, offset)} pairs can collide) and <em>order-preserving</em> (the
 * byte-lexicographic order of the encoding equals {@link #compareTo(IngestionShardPointer)}: key bytes compared
 * lexicographically, then the fixed-width offset). The key is escaped by replacing each {@code 0x00} byte with the
 * two-byte sequence {@code 0x00 0x01} and terminated by {@code 0x00 0x00}; because {@code 0x00 0x00} sorts before
 * both an escaped null ({@code 0x00 0x01}) and any real key byte, the terminator preserves prefix ordering. A
 * fixed-width big-endian offset (byte positions are non-negative) preserves numeric order for a fixed key. Because the
 * encoding is order-preserving, {@link #compareTo(IngestionShardPointer)} is an unsigned byte comparison of it.
 */
public final class ObjectStorePointer implements IngestionShardPointer {

    private static final byte NULL = (byte) 0x00;
    private static final byte NULL_ESCAPE = (byte) 0x01;

    private final String objectKey;
    private final long byteOffset;

    private final byte[] encoded;
    private final String asString;

    /**
     * Creates a pointer.
     *
     * @param objectKey  the object key this pointer refers to; must not be {@code null}
     * @param byteOffset the byte offset of the next unread byte within the object; must not be negative
     */
    public ObjectStorePointer(String objectKey, long byteOffset) {
        this.objectKey = Objects.requireNonNull(objectKey, "objectKey must not be null");
        if (byteOffset < 0) {
            throw new IllegalArgumentException("byteOffset must not be negative, got: " + byteOffset);
        }
        this.byteOffset = byteOffset;
        this.encoded = encode(objectKey, byteOffset);
        this.asString = Base64.getUrlEncoder().withoutPadding().encodeToString(encoded);
    }

    /**
     * Returns the object key this pointer refers to.
     *
     * @return the object key
     */
    public String getObjectKey() {
        return objectKey;
    }

    /**
     * Returns the byte offset within the object.
     *
     * @return the byte offset
     */
    public long getByteOffset() {
        return byteOffset;
    }

    @Override
    public byte[] serialize() {
        return encoded.clone();
    }

    /**
     * Builds the byte encoding once at construction. The framework asks for it several times per indexed document (as a
     * point field, as the {@code _id} string, and in poller logging) from more than one thread, so it is computed
     * eagerly into a final field rather than lazily, which would publish the array unsafely.
     *
     * @param objectKey  the object key
     * @param byteOffset the byte offset
     * @return the encoding
     */
    private static byte[] encode(String objectKey, long byteOffset) {
        byte[] keyBytes = objectKey.getBytes(StandardCharsets.UTF_8);
        int nulls = 0;
        for (byte b : keyBytes) {
            if (b == NULL) {
                nulls++;
            }
        }
        byte[] out = new byte[keyBytes.length + nulls + 2 + Long.BYTES];
        int position = 0;
        for (byte b : keyBytes) {
            out[position++] = b;
            if (b == NULL) {
                out[position++] = NULL_ESCAPE;
            }
        }
        out[position++] = NULL;
        out[position++] = NULL;
        for (int shift = Long.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
            out[position++] = (byte) (byteOffset >>> shift);
        }
        return out;
    }

    /**
     * Deserializes a pointer from its {@link #serialize()} byte encoding.
     *
     * @param bytes the serialized bytes
     * @return the deserialized pointer
     * @throws IllegalArgumentException if the bytes are not a valid pointer encoding
     */
    public static ObjectStorePointer deserialize(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        byte[] key = new byte[bytes.length];
        int keyLength = 0;
        int i = 0;
        boolean terminated = false;
        while (i < bytes.length) {
            byte b = bytes[i];
            if (b == NULL) {
                if (i + 1 >= bytes.length) {
                    throw new IllegalArgumentException("Malformed pointer: dangling escape byte");
                }
                byte next = bytes[i + 1];
                if (next == NULL) {
                    i += 2;
                    terminated = true;
                    break;
                } else if (next == NULL_ESCAPE) {
                    key[keyLength++] = NULL;
                    i += 2;
                } else {
                    throw new IllegalArgumentException(
                        "Malformed pointer: invalid escape sequence 0x00 0x" + Integer.toHexString(next & 0xFF)
                    );
                }
            } else {
                key[keyLength++] = b;
                i++;
            }
        }
        if (terminated == false) {
            throw new IllegalArgumentException("Malformed pointer: missing key terminator");
        }
        if (bytes.length - i != Long.BYTES) {
            throw new IllegalArgumentException("Malformed pointer: expected " + Long.BYTES + " offset bytes, found " + (bytes.length - i));
        }
        long offset = 0L;
        for (int j = 0; j < Long.BYTES; j++) {
            offset = (offset << Byte.SIZE) | (bytes[i + j] & 0xFFL);
        }
        return new ObjectStorePointer(new String(key, 0, keyLength, StandardCharsets.UTF_8), offset);
    }

    @Override
    public String asString() {
        return asString;
    }

    /**
     * Parses a pointer from its {@link #asString()} representation.
     *
     * @param value the string representation
     * @return the parsed pointer
     * @throws IllegalArgumentException if the value is not a valid pointer string
     */
    public static ObjectStorePointer fromString(String value) {
        Objects.requireNonNull(value, "pointer string must not be null");
        try {
            return deserialize(Base64.getUrlDecoder().decode(value));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("malformed object_store pointer [" + LogSanitizer.sanitize(value) + "]", e);
        }
    }

    @Override
    public Field asPointField(String fieldName) {
        return new KeywordField(fieldName, new BytesRef(encoded.clone()), Field.Store.YES);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        return new TermRangeQuery(fieldName, new BytesRef(encoded.clone()), null, false, true);
    }

    @Override
    public int compareTo(IngestionShardPointer o) {
        Objects.requireNonNull(o, "the pointer must not be null");
        if ((o instanceof ObjectStorePointer) == false) {
            throw new ClassCastException("the pointer is of type " + o.getClass() + " and not ObjectStorePointer");
        }
        return Arrays.compareUnsigned(encoded, ((ObjectStorePointer) o).encoded);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectStorePointer that = (ObjectStorePointer) o;
        return byteOffset == that.byteOffset && objectKey.equals(that.objectKey);
    }

    @Override
    public int hashCode() {
        return 31 * objectKey.hashCode() + Long.hashCode(byteOffset);
    }

    @Override
    public String toString() {
        return "ObjectStorePointer{objectKey='" + objectKey + "', byteOffset=" + byteOffset + '}';
    }
}
