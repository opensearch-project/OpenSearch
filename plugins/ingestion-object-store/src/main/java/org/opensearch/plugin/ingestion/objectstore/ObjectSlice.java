/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import java.util.Objects;

/**
 * A contiguous byte range of a single object that a shard owns and must read, resolved to line boundaries, together with
 * the object's declared size. For whole-object split strategies the range spans the entire object
 * ({@code startOffset == 0} and {@code endOffsetExclusive == objectSize}). Under {@code equal_bytes} only the slice that
 * reaches the end of the object has {@code endOffsetExclusive == objectSize}; the declared size lets the reader tell that
 * slice apart from one that ends where the next shard's slice begins.
 */
final class ObjectSlice {

    private final String objectName;
    private final long startOffset;
    private final long endOffsetExclusive;
    private final long objectSize;

    /**
     * Creates an object slice.
     *
     * @param objectName         the object name within the container; must not be {@code null}
     * @param startOffset        the inclusive start byte offset (a resolved line boundary); must not be negative
     * @param endOffsetExclusive the exclusive end byte offset (a resolved line boundary or the object size)
     * @param objectSize         the object's declared size (from the manifest or the listing); must not be less than
     *                           {@code endOffsetExclusive}
     */
    public ObjectSlice(String objectName, long startOffset, long endOffsetExclusive, long objectSize) {
        this.objectName = Objects.requireNonNull(objectName, "objectName must not be null");
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must not be negative, got: " + startOffset);
        }
        if (endOffsetExclusive < startOffset) {
            throw new IllegalArgumentException(
                "endOffsetExclusive [" + endOffsetExclusive + "] must not be less than startOffset [" + startOffset + "]"
            );
        }
        if (objectSize < endOffsetExclusive) {
            throw new IllegalArgumentException(
                "objectSize [" + objectSize + "] must not be less than endOffsetExclusive [" + endOffsetExclusive + "]"
            );
        }
        this.startOffset = startOffset;
        this.endOffsetExclusive = endOffsetExclusive;
        this.objectSize = objectSize;
    }

    /**
     * Returns the object name.
     *
     * @return the object name
     */
    public String getObjectName() {
        return objectName;
    }

    /**
     * Returns the inclusive start offset.
     *
     * @return the start offset
     */
    public long getStartOffset() {
        return startOffset;
    }

    /**
     * Returns the exclusive end offset.
     *
     * @return the exclusive end offset
     */
    public long getEndOffsetExclusive() {
        return endOffsetExclusive;
    }

    /**
     * Returns the object's declared size.
     *
     * @return the declared object size
     */
    public long getObjectSize() {
        return objectSize;
    }

    /**
     * Returns whether this slice ends at the object's declared end, so that any byte beyond it means the object is larger
     * than declared.
     *
     * @return {@code true} if {@code endOffsetExclusive == objectSize}
     */
    public boolean endsAtObjectEnd() {
        return endOffsetExclusive == objectSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectSlice that = (ObjectSlice) o;
        return startOffset == that.startOffset
            && endOffsetExclusive == that.endOffsetExclusive
            && objectSize == that.objectSize
            && objectName.equals(that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectName, startOffset, endOffsetExclusive, objectSize);
    }

    @Override
    public String toString() {
        return "ObjectSlice{objectName='"
            + objectName
            + "', startOffset="
            + startOffset
            + ", endOffsetExclusive="
            + endOffsetExclusive
            + ", objectSize="
            + objectSize
            + '}';
    }
}
