/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Resolves a nominal byte cut into a true line-start offset so that byte ranges assigned to different shards meet
 * exactly at line boundaries with no lost or duplicated record.
 *
 * <p>The rule mirrors the standard split-file convention: offset {@code 0} is always a line start; for any nominal
 * offset {@code S > 0} the resolved line start is the byte immediately after the first {@code \n} at or after
 * {@code S} (the partial line straddling {@code S} belongs to the previous shard, which reads past its own end to
 * finish it). If no newline exists between {@code S} and the end of the object, the object has no line starting in
 * this region and the resolved start is the object size (this shard owns nothing).
 *
 * <p>The scan is bounded: a splittable ndjson object has a newline at least every {@code maxScanBytes}, so if the scan
 * consumes that many bytes without finding one while the object still has bytes left, the object violates the
 * precondition and an {@link UnresolvableBoundaryException} is raised rather than scanning gigabytes. The bound is not
 * open-ended: the only caller, {@link SliceAssignment}, passes {@link NdjsonFramer#maxLineBytes} of the configured
 * {@code max_record_bytes} (10 MB by default) with the BOM allowance, which also covers the terminating {@code \n} of a
 * line that starts exactly at the cut, so the furthest this can ever read is one record's worth of bytes.
 */
final class BoundaryResolver {

    private static final int LF = '\n';
    private static final int READ_BUFFER = 8192;

    private BoundaryResolver() {}

    /**
     * Resolves a nominal byte offset to the next true line start.
     *
     * @param container      the blob container holding the object
     * @param objectName     the object name within the container
     * @param nominalOffset  the nominal byte cut
     * @param objectSize     the total object size in bytes
     * @param maxScanBytes  the maximum number of bytes to scan for a newline
     * @return the resolved line-start offset in {@code [0, objectSize]}
     * @throws UnresolvableBoundaryException if no newline is found within {@code maxScanBytes}, or the object delivers
     *                                       fewer bytes than {@code objectSize} implies
     * @throws IOException                   if the object cannot be read
     * @throws IllegalArgumentException      if {@code maxScanBytes} is not positive
     */
    /**
     * Resolves a nominal byte offset to the next true line start.
     *
     * @param container      the blob container holding the object
     * @param objectName     the object name within the container
     * @param nominalOffset  the nominal byte cut
     * @param objectSize     the total object size in bytes
     * @param maxScanBytes  the maximum number of bytes to scan for a newline
     * @return the resolved line-start offset in {@code [0, objectSize]}
     * @throws UnresolvableBoundaryException if no newline is found within {@code maxScanBytes}, or the object delivers
     *                                       fewer bytes than {@code objectSize} implies
     * @throws IOException                   if the object cannot be read
     * @throws IllegalArgumentException      if {@code maxScanBytes} is not positive
     */
    public static long resolveLineStart(BlobContainer container, String objectName, long nominalOffset, long objectSize, long maxScanBytes)
        throws IOException {
        return resolveLineStart(container, objectName, nominalOffset, objectSize, maxScanBytes, new StoreReadTally());
    }

    /**
     * Resolves a nominal byte offset to the next true line start, charging any read it issues to the given tally so a
     * cost cap can account for work done before the reading consumer exists.
     *
     * @param container      the blob container holding the object
     * @param objectName     the object name within the container
     * @param nominalOffset  the nominal byte cut
     * @param objectSize     the total object size in bytes
     * @param maxScanBytes   the maximum number of bytes to scan for a newline
     * @param tally          collects the get request and the bytes delivered, so a cost cap can account for them
     * @return the resolved line-start offset in {@code [0, objectSize]}
     * @throws UnresolvableBoundaryException if no newline is found within {@code maxScanBytes}, or the object delivers
     *                                       fewer bytes than {@code objectSize} implies
     * @throws IOException                   if the object cannot be read
     * @throws IllegalArgumentException      if {@code maxScanBytes} is not positive
     */
    public static long resolveLineStart(
        BlobContainer container,
        String objectName,
        long nominalOffset,
        long objectSize,
        long maxScanBytes,
        StoreReadTally tally
    ) throws IOException {
        Objects.requireNonNull(container, "container must not be null");
        Objects.requireNonNull(tally, "tally must not be null");
        Objects.requireNonNull(objectName, "objectName must not be null");
        if (maxScanBytes <= 0) {
            throw new IllegalArgumentException("maxScanBytes must be greater than 0, got [" + maxScanBytes + "]");
        }
        if (nominalOffset <= 0) {
            return 0;
        }
        if (nominalOffset >= objectSize) {
            return objectSize;
        }
        long remaining = objectSize - nominalOffset;
        long requested = Math.min(remaining, maxScanBytes);
        long lineStart = -1;
        long scanned = 0;
        try (InputStream in = container.readBlob(objectName, nominalOffset, requested)) {
            byte[] buffer = new byte[(int) Math.min(READ_BUFFER, requested)];
            int n;
            while (lineStart < 0
                && scanned < requested
                && (n = in.read(buffer, 0, (int) Math.min(buffer.length, requested - scanned))) != -1) {
                for (int i = 0; i < n; i++) {
                    if (buffer[i] == LF) {
                        lineStart = nominalOffset + scanned + i + 1;
                        break;
                    }
                }
                scanned += n;
            }
        } finally {
            tally.recordRead(scanned);
        }
        if (lineStart >= 0) {
            return lineStart;
        }
        if (scanned < requested) {
            throw new UnresolvableBoundaryException(
                "object ["
                    + LogSanitizer.sanitize(objectName)
                    + "] delivered ["
                    + scanned
                    + "] bytes from offset ["
                    + nominalOffset
                    + "] but the frozen manifest implies ["
                    + requested
                    + "]; the object was truncated and the write-once precondition was violated"
            );
        }
        if (requested < remaining) {
            throw new UnresolvableBoundaryException(
                "object ["
                    + LogSanitizer.sanitize(objectName)
                    + "] has no newline within ["
                    + maxScanBytes
                    + "] of offset ["
                    + nominalOffset
                    + "]; the object is not splittable ndjson"
            );
        }
        return objectSize;
    }
}
