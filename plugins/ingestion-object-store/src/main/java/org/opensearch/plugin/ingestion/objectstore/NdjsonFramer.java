/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Frames a newline-delimited (ndjson) byte stream into per-record byte slices, applying the byte-boundary
 * ownership rule described below so that every record has exactly one owning shard and no record is lost or
 * double-counted.
 *
 * <p>The stream passed to {@link #frame} must begin at a true line boundary (the caller resolves a nominal byte
 * cut to a line start; see {@link BoundaryResolver}). A record is <em>owned</em> if and only if the first byte of
 * its line falls in {@code [startOffset, sliceEndExclusive)}. The framer reads past {@code sliceEndExclusive} to
 * finish a straddling line, then stops before emitting the first line that begins at or after
 * {@code sliceEndExclusive} (that line belongs to the next shard).
 *
 * <p>Details: {@code \r\n} is handled by stripping a trailing {@code \r}; a UTF-8 BOM is stripped only when the
 * caller indicates this reader owns absolute offset 0; blank lines (empty after stripping) are skipped but their
 * bytes are consumed and reflected in {@link FramingResult#skippedBlankLines()}.
 *
 * <p>{@code maxRecordBytes} caps the <em>payload</em>, the record bytes after the trailing {@code \r} and any leading
 * BOM have been stripped, so the cap is independent of the line terminator the producer used. A longer record raises
 * an {@link IOException} instead of buffering without bound.
 */
final class NdjsonFramer {

    private static final int LF = '\n';
    private static final int CR = '\r';
    private static final byte BOM_0 = (byte) 0xEF;
    private static final byte BOM_1 = (byte) 0xBB;
    private static final byte BOM_2 = (byte) 0xBF;

    /**
     * Bytes a UTF-8 byte-order mark occupies, and so the most {@link #toRecord} can strip from the front of a line.
     */
    private static final int BOM_BYTES = 3;

    /**
     * Bytes a carriage return occupies, and so the most {@link #toRecord} can strip from the end of a line.
     */
    private static final int CR_BYTES = 1;

    /**
     * Bytes pulled from the stream per read. Framing scans this buffer for the next newline and copies the whole span
     * before it in one call, rather than reading and appending a byte at a time, so the cost of both the stream read
     * and the accumulating buffer is paid per line rather than per byte.
     */
    private static final int READ_CHUNK_BYTES = 8192;

    private NdjsonFramer() {}

    /**
     * A single framed record with its absolute start offset and the absolute offset of the next byte after its
     * terminating newline.
     */
    public static final class FramedRecord {
        private final byte[] payload;
        private final long startOffset;
        private final long nextOffset;

        /**
         * Creates a framed record.
         *
         * @param payload     the record bytes with any trailing {@code \r} and leading BOM removed
         * @param startOffset the absolute byte offset at which this record's line begins
         * @param nextOffset  the absolute byte offset immediately after this record's terminating newline (or the
         *                    object end if the record had no trailing newline)
         */
        public FramedRecord(byte[] payload, long startOffset, long nextOffset) {
            this.payload = payload;
            this.startOffset = startOffset;
            this.nextOffset = nextOffset;
        }

        /**
         * Returns the record bytes.
         *
         * @return the payload
         */
        public byte[] getPayload() {
            return payload == null ? null : payload.clone();
        }

        /**
         * Returns the absolute start offset of this record's line.
         *
         * @return the start offset
         */
        public long getStartOffset() {
            return startOffset;
        }

        /**
         * Returns the absolute offset of the next unread byte after this record.
         *
         * @return the next offset
         */
        public long getNextOffset() {
            return nextOffset;
        }
    }

    /**
     * The outcome of a framing pass: the owned records, the count of blank lines skipped, and whether framing
     * stopped because it reached the record limit (as opposed to reaching the slice end or the end of the stream).
     */
    public static final class FramingResult {
        private final List<FramedRecord> records;
        private final long skippedBlankLines;
        private final boolean limitReached;

        /**
         * Creates a framing result.
         *
         * @param records           the owned framed records in order
         * @param skippedBlankLines the number of blank lines skipped
         * @param limitReached      whether framing stopped because the record limit was reached
         */
        public FramingResult(List<FramedRecord> records, long skippedBlankLines, boolean limitReached) {
            this.records = records;
            this.skippedBlankLines = skippedBlankLines;
            this.limitReached = limitReached;
        }

        /**
         * Returns the owned records in stream order.
         *
         * @return the records
         */
        public List<FramedRecord> records() {
            return records;
        }

        /**
         * Returns the number of blank lines skipped during framing.
         *
         * @return the skipped blank line count
         */
        public long skippedBlankLines() {
            return skippedBlankLines;
        }

        /**
         * Returns whether framing stopped because it reached the requested record limit. When {@code false}, framing
         * consumed the stream until the slice end or the end of the stream, so a short read indicates a truncated
         * object.
         *
         * @return {@code true} if the record limit was reached
         */
        public boolean limitReached() {
            return limitReached;
        }
    }

    /**
     * The framing parameters for one pass: where the stream begins, where this shard's ownership ends, whether it owns
     * absolute offset 0, the per-record payload cap, and how many records to emit.
     */
    public static final class FramingOptions {
        private final long startOffset;
        private final long sliceEndExclusive;
        private final boolean ownsOffsetZero;
        private final long maxRecordBytes;
        private final long maxRecords;

        /**
         * Creates the options.
         *
         * @param startOffset       the absolute offset at which the stream begins; must be a line start
         * @param sliceEndExclusive the exclusive end of this shard's owned range, {@link Long#MAX_VALUE} for the last
         * @param ownsOffsetZero    whether this reader owns absolute offset 0 and should strip a leading UTF-8 BOM
         * @param maxRecordBytes    the hard per-record payload byte cap
         * @param maxRecords        the maximum number of owned records to emit, or {@code -1} for no limit
         */
        public FramingOptions(long startOffset, long sliceEndExclusive, boolean ownsOffsetZero, long maxRecordBytes, long maxRecords) {
            this.startOffset = startOffset;
            this.sliceEndExclusive = sliceEndExclusive;
            this.ownsOffsetZero = ownsOffsetZero;
            this.maxRecordBytes = maxRecordBytes;
            this.maxRecords = maxRecords;
        }

        long startOffset() {
            return startOffset;
        }

        long sliceEndExclusive() {
            return sliceEndExclusive;
        }

        boolean ownsOffsetZero() {
            return ownsOffsetZero;
        }

        long maxRecordBytes() {
            return maxRecordBytes;
        }

        long maxRecords() {
            return maxRecords;
        }
    }

    /**
     * Frames records from a stream that begins at a true line boundary.
     *
     * @param in      the input stream, positioned at the options' start offset within the object
     * @param options the framing parameters
     * @return the framing result
     * @throws IOException if the stream fails or a record's payload exceeds {@code maxRecordBytes}
     */
    public static FramingResult frame(InputStream in, FramingOptions options) throws IOException {
        final long sliceEndExclusive = options.sliceEndExclusive();
        final boolean ownsOffsetZero = options.ownsOffsetZero();
        final long maxRecordBytes = options.maxRecordBytes();
        final long maxRecords = options.maxRecords();
        List<FramedRecord> records = new ArrayList<>();
        long skippedBlank = 0;
        if (maxRecords == 0) {
            return new FramingResult(records, skippedBlank, true);
        }
        long lineStart = options.startOffset();
        long absolute = options.startOffset();
        boolean firstLine = true;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final long bufferCeiling = maxLineBytes(maxRecordBytes, ownsOffsetZero);
        final byte[] chunk = new byte[READ_CHUNK_BYTES];
        int chunkLength = 0;
        int chunkPosition = 0;

        while (lineStart < sliceEndExclusive) {
            if (chunkPosition == chunkLength) {
                chunkLength = in.read(chunk, 0, chunk.length);
                if (chunkLength == -1) {
                    break;
                }
                chunkPosition = 0;
            }
            final int newline = indexOfNewline(chunk, chunkPosition, chunkLength);
            final int spanEnd = newline < 0 ? chunkLength : newline;
            if (spanEnd > chunkPosition) {
                buffer.write(chunk, chunkPosition, spanEnd - chunkPosition);
                absolute += spanEnd - chunkPosition;
                chunkPosition = spanEnd;
                if (buffer.size() > bufferCeiling) {
                    throw tooLarge(lineStart, maxRecordBytes);
                }
            }
            if (newline < 0) {
                continue;
            }
            chunkPosition++;
            FramedRecord record = toRecord(buffer, lineStart, absolute + 1, firstLine && ownsOffsetZero, maxRecordBytes);
            if (record == null) {
                skippedBlank++;
            } else {
                records.add(record);
                if (maxRecords >= 0 && records.size() >= maxRecords) {
                    return new FramingResult(records, skippedBlank, true);
                }
            }
            firstLine = false;
            buffer.reset();
            absolute++;
            lineStart = absolute;
        }

        if (buffer.size() > 0) {
            FramedRecord tail = toRecord(buffer, lineStart, absolute, firstLine && ownsOffsetZero, maxRecordBytes);
            if (tail == null) {
                skippedBlank++;
            } else {
                records.add(tail);
            }
        }
        return new FramingResult(records, skippedBlank, false);
    }

    /**
     * Returns the maximum number of bytes a single line may occupy for a given payload cap: the cap plus the most
     * {@link #toRecord} can strip from it (a trailing {@code \r}, plus a three-byte UTF-8 BOM where BOM stripping applies),
     * saturating so an unbounded cap cannot overflow. {@link BoundaryResolver} bounds its newline scan with the same
     * function so the two cannot disagree about how far apart two newlines may legitimately be.
     *
     * @param maxRecordBytes the per-record payload byte cap
     * @param stripsBom      whether BOM stripping applies (the reader owns absolute offset 0)
     * @return the maximum permitted line length
     */
    static long maxLineBytes(long maxRecordBytes, boolean stripsBom) {
        long allowance = stripsBom ? CR_BYTES + BOM_BYTES : CR_BYTES;
        return maxRecordBytes > Long.MAX_VALUE - allowance ? Long.MAX_VALUE : maxRecordBytes + allowance;
    }

    private static int indexOfNewline(byte[] chunk, int from, int to) {
        for (int i = from; i < to; i++) {
            if (chunk[i] == LF) {
                return i;
            }
        }
        return -1;
    }

    private static IOException tooLarge(long lineStart, long maxRecordBytes) {
        return new IOException("record starting at offset " + lineStart + " exceeds max_record_bytes [" + maxRecordBytes + "]");
    }

    /**
     * Builds the record a completed line yields, or {@code null} when the line is blank once its terminator and any
     * leading BOM have been stripped.
     *
     * @param buffer         the accumulated line bytes
     * @param lineStart      the absolute offset at which the line begins
     * @param nextOffset     the absolute offset immediately after the line
     * @param stripBom       whether a leading UTF-8 BOM should be removed
     * @param maxRecordBytes the hard per-record payload byte cap
     * @return the framed record, or {@code null} if the line is blank
     * @throws IOException if the payload exceeds {@code maxRecordBytes}
     */
    private static FramedRecord toRecord(
        ByteArrayOutputStream buffer,
        long lineStart,
        long nextOffset,
        boolean stripBom,
        long maxRecordBytes
    ) throws IOException {
        byte[] raw = buffer.toByteArray();
        int from = 0;
        int to = raw.length;
        if (to > 0 && raw[to - 1] == CR) {
            to--;
        }
        if (stripBom && to - from >= BOM_BYTES && raw[from] == BOM_0 && raw[from + 1] == BOM_1 && raw[from + 2] == BOM_2) {
            from += BOM_BYTES;
        }
        if (to - from <= 0) {
            return null;
        }
        if (to - from > maxRecordBytes) {
            throw tooLarge(lineStart, maxRecordBytes);
        }
        byte[] payload = (from == 0 && to == raw.length) ? raw : Arrays.copyOfRange(raw, from, to);
        return new FramedRecord(payload, lineStart, nextOffset);
    }
}
