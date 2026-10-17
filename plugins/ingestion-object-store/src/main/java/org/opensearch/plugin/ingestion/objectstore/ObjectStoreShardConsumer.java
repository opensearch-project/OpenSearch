/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.plugin.ingestion.objectstore.NdjsonFramer.FramedRecord;
import org.opensearch.plugin.ingestion.objectstore.NdjsonFramer.FramingResult;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Consumes the disjoint slice assigned to a single shard. It resolves the shard's owned {@link ObjectSlice}s from
 * the frozen manifest once at construction, then reads byte ranges through a {@link BlobContainer}, frames ndjson
 * records with {@link NdjsonFramer}, and emits them as {@link ObjectStoreMessage} paired with an
 * {@link ObjectStorePointer} at the record's start offset. Reads proceed across the shard's slices in order.
 *
 * <p>By default the consumer performs no LIST: the object set and every object's size come from the frozen manifest.
 * Under {@code discovery=list} the shard lists its own sub-prefix once at construction (see {@link ListDiscovery}) and
 * issues only ranged GETs thereafter.
 *
 * <p>Three defenses are enforced during reads. The cost caps ({@code max_get_requests}, {@code max_bytes}) bound the
 * cumulative get requests and bytes read; exceeding either raises {@link CostLimitExceededException}. The leading bytes
 * of each object are checked for a gzip magic number. The
 * write-once precondition is enforced as a read-based size check: each object's size is known up front, so a slice
 * frames exactly the bytes that size implies, and a stream that ends early fails the read rather than indexing shifted
 * bytes. Each slice carries its object's declared size, and the request for a slice that ends at that size asks for one
 * byte beyond it; if the store delivers it, the object is larger than its manifest size and the read fails rather than
 * emitting a cut-off last record and silently dropping the rest. A slice that ends before its object's declared size
 * (an earlier {@code equal_bytes} slice, which ends where the next shard's slice begins) requests exactly its own range
 * and is never checked. An object declared empty has no range to frame, so its slice reads one byte instead and fails
 * the same way if the store delivers it. Untrusted object keys are sanitized with {@link LogSanitizer} before being logged.
 *
 * <p>{@code timeoutMillis} bounds a poll at slice granularity: the read loop stops before starting another slice once
 * the deadline has passed and returns the records gathered so far, rather than discarding them.
 *
 * <p>The stateless {@code readNext(maxMessages, timeoutMillis)} overload resumes from the <b>next unread offset</b>
 * rather than from the last record it returned. That matters once a shard has drained its slices: the resume offset then
 * equals the slice end, every slice is skipped without opening a stream, and a completed shard issues no further get
 * requests. Resuming from the last record's start instead would re-read and re-skip it on every poll for the life of the
 * index.
 */
public class ObjectStoreShardConsumer implements IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> {

    private static final Logger logger = LogManager.getLogger(ObjectStoreShardConsumer.class);

    private static final ObjectStorePointer EMPTY_POINTER = new ObjectStorePointer("", 0);

    private static final int GZIP_MAGIC_0 = 0x1f;
    private static final int GZIP_MAGIC_1 = 0x8b;
    private static final int GZIP_MAGIC_BYTES = 2;

    private final BlobContainer container;
    private final List<ObjectSlice> slices;
    private final Map<String, Integer> sliceIndexByObject;
    private final long maxRecordBytes;
    private final long maxGetRequests;
    private final long maxBytes;
    private final int shardId;

    private final BitSet verifiedObjects;
    private final BitSet verifiedEmptySlices;
    private long getRequestCount;
    private long bytesRead;

    private ObjectStorePointer progress;

    /**
     * Creates a shard consumer over the slices this shard owns.
     *
     * @param config    the parsed source configuration
     * @param container the blob container to read from
     * @param shardId   the shard id this consumer serves
     * @param slices    the ordered, disjoint slices this shard owns, as resolved by {@link SliceAssignment}
     */
    public ObjectStoreShardConsumer(ObjectStoreSourceConfig config, BlobContainer container, int shardId, List<ObjectSlice> slices) {
        this.container = Objects.requireNonNull(container, "container must not be null");
        this.slices = List.copyOf(Objects.requireNonNull(slices, "slices must not be null"));
        this.shardId = shardId;
        Objects.requireNonNull(config, "config must not be null");
        this.maxRecordBytes = config.getMaxRecordBytes();
        this.maxGetRequests = config.getMaxGetRequests();
        this.maxBytes = config.getMaxBytes();
        if (shardId == 0
            && (maxBytes == Long.MAX_VALUE || maxGetRequests == Long.MAX_VALUE || config.getMaxListRequests() == Long.MAX_VALUE)) {
            logger.warn(
                "object_store cost caps are unbounded (max_bytes/max_get_requests/max_list_requests default to unlimited); "
                    + "set them to guard against runaway cloud cost"
            );
        }
        Map<String, Integer> index = new HashMap<>((int) (slices.size() / 0.75f) + 1);
        for (int i = 0; i < slices.size(); i++) {
            index.putIfAbsent(slices.get(i).getObjectName(), i);
        }
        this.sliceIndexByObject = index;
        this.verifiedObjects = new BitSet(slices.size());
        this.verifiedEmptySlices = new BitSet(slices.size());
    }

    @Override
    public List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> readNext(
        ObjectStorePointer pointer,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        Objects.requireNonNull(pointer, "pointer must not be null");
        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results = new ArrayList<>();
        if (slices.isEmpty()) {
            return results;
        }
        final long limit = maxMessages < 0 ? Long.MAX_VALUE : maxMessages;
        if (limit == 0) {
            return results;
        }

        int startIdx = sliceIndexForObject(pointer.getObjectKey());
        if (startIdx < 0) {
            throw new PointerNotOwnedException(
                "pointer ["
                    + LogSanitizer.sanitize(pointer.asString())
                    + "] refers to object ["
                    + LogSanitizer.sanitize(pointer.getObjectKey())
                    + "] which is not among the "
                    + slices.size()
                    + " slices owned by shard "
                    + shardId
                    + "; the likely cause is that the source layout or manifest changed after ingestion started (for example the ["
                    + ObjectStoreSourceConfig.MANIFEST
                    + "], ["
                    + ObjectStoreSourceConfig.SOURCE_URL
                    + "], ["
                    + ObjectStoreSourceConfig.SOURCE_PREFIX
                    + "], ["
                    + ObjectStoreSourceConfig.SHARD_SPLIT
                    + "] or ["
                    + ObjectStoreSourceConfig.DISCOVERY
                    + "] param was updated on a live index), which is not supported; recreate the index to ingest the new layout"
            );
        }
        boolean skipStartRecord = includeStart == false;
        long deadlineNanos = timeoutMillis > 0 ? nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis) : 0;

        ObjectStorePointer next = null;
        try {
            for (int i = startIdx; i < slices.size() && results.size() < limit; i++) {
                if (deadlineNanos != 0 && nanoTime() - deadlineNanos >= 0) {
                    break;
                }
                ObjectStorePointer sliceProgress = readSlice(i, pointer, skipStartRecord, limit, results);
                if (sliceProgress != null) {
                    next = sliceProgress;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read from object storage for shard " + shardId + ": " + e.getMessage(), e);
        }
        if (next != null) {
            progress = next;
        } else if (progress == null) {
            progress = pointer;
        }
        return results;
    }

    /**
     * Reads one slice and appends the owned records it yields, up to the caller's overall record limit. The pointer is
     * matched against the slice by object name, so a slice is resumed from the pointer's offset only when the pointer
     * actually addresses that slice's object.
     *
     * <p>The resume offset this slice reached is returned rather than recorded, so the caller publishes it only once the
     * whole poll has succeeded. A later slice that fails discards the gathered records, and the resume offset must then
     * stay where it was or those records would never be delivered.
     *
     * @param sliceIndex     the index of the slice to read
     * @param pointer        the pointer the read resumes from
     * @param skipStartRecord whether the record starting exactly at the pointer must be excluded
     * @param limit          the total number of records {@code results} may hold
     * @param results        the accumulating result list
     * @return the next unread offset after this slice's returned records, or {@code null} if the slice was already
     *         exhausted and the resume offset is unchanged
     * @throws IOException if the object cannot be read, is compressed, was truncated, or is larger than its manifest size
     */
    private ObjectStorePointer readSlice(
        int sliceIndex,
        ObjectStorePointer pointer,
        boolean skipStartRecord,
        long limit,
        List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> results
    ) throws IOException {
        ObjectSlice slice = slices.get(sliceIndex);
        boolean addressesSlice = ownsPointerObject(slice, pointer);
        long frameStart = addressesSlice ? Math.max(pointer.getByteOffset(), slice.getStartOffset()) : slice.getStartOffset();
        if (frameStart >= slice.getEndOffsetExclusive()) {
            if (slice.getObjectSize() == 0) {
                verifyDeclaredEmpty(sliceIndex, slice);
            }
            return null;
        }
        boolean atObjectStart = frameStart == 0;
        if (atObjectStart == false) {
            verifyNotCompressedByProbe(slice.getObjectName());
        }
        boolean skipFirst = addressesSlice && skipStartRecord;
        long remaining = limit - results.size();
        long ask = remaining == Long.MAX_VALUE ? -1 : remaining + (skipFirst ? 1 : 0);
        long length = slice.getEndOffsetExclusive() - frameStart;
        boolean checkBeyondEnd = slice.endsAtObjectEnd();
        long requested = checkBeyondEnd ? Math.max(length, length + 1) : length;
        getRequestCount++;
        enforceGetRequestCap();
        try (
            InputStream raw = container.readBlob(slice.getObjectName(), frameStart, requested);
            BufferedInputStream buffered = new BufferedInputStream(raw);
            CountingInputStream counting = new CountingInputStream(buffered, length, remainingByteBudget())
        ) {
            if (atObjectStart) {
                verifyNotCompressedFromStream(slice.getObjectName(), buffered);
            }
            FramingResult framed = NdjsonFramer.frame(
                counting,
                new NdjsonFramer.FramingOptions(frameStart, slice.getEndOffsetExclusive(), atObjectStart, maxRecordBytes, ask)
            );
            if (framed.limitReached() == false && counting.count() < length) {
                throw new IOException(
                    "object ["
                        + LogSanitizer.sanitize(slice.getObjectName())
                        + "] delivered ["
                        + counting.count()
                        + "] bytes for the range starting at ["
                        + frameStart
                        + "] but the frozen manifest implies ["
                        + length
                        + "]; the object was truncated and the write-once precondition was violated"
                );
            }
            bytesRead += counting.count();
            enforceBytesCap();
            List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> owned = new ArrayList<>();
            long reached = frameStart;
            boolean advanced = false;
            for (FramedRecord record : framed.records()) {
                if (skipFirst && record.getStartOffset() == pointer.getByteOffset()) {
                    skipFirst = false;
                    continue;
                }
                if (results.size() + owned.size() >= limit) {
                    break;
                }
                ObjectStorePointer recordPointer = new ObjectStorePointer(slice.getObjectName(), record.getStartOffset());
                owned.add(new ReadResult<>(recordPointer, new ObjectStoreMessage(record.getPayload(), currentTimeMillis())));
                reached = record.getNextOffset();
                advanced = true;
            }
            if (framed.limitReached() == false) {
                reached = slice.getEndOffsetExclusive();
                advanced = true;
            }
            ObjectStorePointer sliceProgress = advanced ? new ObjectStorePointer(slice.getObjectName(), reached) : null;
            if (checkBeyondEnd && reached == slice.getEndOffsetExclusive()) {
                failIfLargerThanDeclared(slice, buffered);
            }
            results.addAll(owned);
            return sliceProgress;
        }
    }

    /**
     * Fails the read if the object holds bytes beyond the declared end of a slice that ends at the object's declared size.
     * The byte beyond was already requested with the slice, so the check costs no extra get request, and only a byte the
     * store actually delivers is billed against {@code max_bytes}. It is called only for a slice whose end equals its
     * object's declared size, whatever that slice's last byte is, and only once the poll reaches that end, including a
     * poll that stops at its record limit with the record ending exactly there. It runs before the slice's records are
     * added to the poll, so a grown object never yields its last records and then stops silently on the next poll.
     *
     * @param slice    the slice whose full range was just framed
     * @param in       the underlying stream, positioned at the slice's declared end
     * @throws IOException if the object is larger than its manifest size
     */
    private void failIfLargerThanDeclared(ObjectSlice slice, InputStream in) throws IOException {
        if (in.read() == -1) {
            return;
        }
        bytesRead++;
        throw new IOException(
            "object ["
                + LogSanitizer.sanitize(slice.getObjectName())
                + "] has bytes beyond offset ["
                + slice.getEndOffsetExclusive()
                + "] where the frozen manifest says it ends; the object is larger than its manifest size and the write-once "
                + "precondition was violated"
        );
    }

    /**
     * Checks that an object the manifest declares empty really is empty, since a slice of a zero-size object has no range
     * to frame and would otherwise be skipped without any read. One byte is read from offset 0; if the store delivers it,
     * the object is larger than its manifest size and the read fails. The read is not ranged, because a ranged GET of an
     * empty object is rejected by some stores (S3 answers 416), and the stream is closed after the first byte. A
     * confirmed-empty slice is remembered, so a drained shard issues no further get requests for it.
     *
     * @param sliceIndex the index of the zero-size slice
     * @param slice      the zero-size slice
     * @throws IOException if the object cannot be read or holds any byte
     */
    private void verifyDeclaredEmpty(int sliceIndex, ObjectSlice slice) throws IOException {
        if (verifiedEmptySlices.get(sliceIndex)) {
            return;
        }
        getRequestCount++;
        enforceGetRequestCap();
        try (InputStream in = container.readBlob(slice.getObjectName())) {
            failIfLargerThanDeclared(slice, in);
        }
        verifiedEmptySlices.set(sliceIndex);
    }

    @Override
    public List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> readNext(long maxMessages, int timeoutMillis) throws TimeoutException {
        return readNext(progress == null ? earliest() : progress, true, maxMessages, timeoutMillis);
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return earliest();
    }

    @Override
    public IngestionShardPointer latestPointer() {
        if (slices.isEmpty()) {
            return EMPTY_POINTER;
        }
        ObjectSlice last = slices.get(slices.size() - 1);
        return new ObjectStorePointer(last.getObjectName(), last.getEndOffsetExclusive());
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        throw new UnsupportedOperationException("reset_by_timestamp is not supported by the object_store source");
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return ObjectStorePointer.fromString(offset);
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        if (slices.isEmpty()) {
            return 0;
        }
        if (progress != null) {
            return remainingBytesFrom(progress);
        }
        if (expectedStartPointer instanceof ObjectStorePointer) {
            return remainingBytesFrom((ObjectStorePointer) expectedStartPointer);
        }
        return remainingBytesFrom(null);
    }

    @Override
    public void close() throws IOException {}

    private ObjectStorePointer earliest() {
        if (slices.isEmpty()) {
            return EMPTY_POINTER;
        }
        ObjectSlice first = slices.get(0);
        return new ObjectStorePointer(first.getObjectName(), first.getStartOffset());
    }

    private int sliceIndexForObject(String objectKey) {
        return sliceIndexByObject.getOrDefault(objectKey, -1);
    }

    private long remainingBytesFrom(ObjectStorePointer from) {
        long lag = 0;
        int startIdx = from == null ? 0 : sliceIndexForObject(from.getObjectKey());
        if (startIdx < 0) {
            startIdx = 0;
        }
        for (int i = startIdx; i < slices.size(); i++) {
            ObjectSlice slice = slices.get(i);
            long effectiveStart = ownsPointerObject(slice, from)
                ? Math.max(slice.getStartOffset(), from.getByteOffset())
                : slice.getStartOffset();
            long bytes = slice.getEndOffsetExclusive() - effectiveStart;
            if (bytes > 0) {
                lag += bytes;
            }
        }
        return lag;
    }

    /**
     * Returns whether a slice is the one the given pointer addresses, by comparing object names rather than slice
     * position. A slice index alone is not sufficient: it identifies only the first slice of an object, and it silently
     * falls back to the first slice when the pointer names an object this shard does not own, which would apply a
     * foreign offset to an unrelated slice.
     *
     * @param slice   the slice under consideration
     * @param pointer the pointer being resolved (may be {@code null})
     * @return {@code true} if the pointer addresses this slice's object
     */
    private static boolean ownsPointerObject(ObjectSlice slice, ObjectStorePointer pointer) {
        return pointer != null && slice.getObjectName().equals(pointer.getObjectKey());
    }

    private static long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Returns the current value of the nanosecond timer used for the poll deadline. Overridable so a test can drive the
     * deadline deterministically instead of sleeping.
     *
     * @return the current nanosecond timer reading
     */
    long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Checks the gzip magic number with an explicit two-byte ranged GET, for a slice that starts mid-object and so never
     * sees absolute offset 0. Only the bytes the store delivers are billed against {@code max_bytes}.
     *
     * @param objectName the object being read
     * @throws IOException if the probe fails or the object is gzip-compressed
     */
    private void verifyNotCompressedByProbe(String objectName) throws IOException {
        int verified = sliceIndexForObject(objectName);
        if (verifiedObjects.get(verified)) {
            return;
        }
        getRequestCount++;
        enforceGetRequestCap();
        int first;
        int second;
        long probed = 0;
        try (InputStream in = container.readBlob(objectName, 0, GZIP_MAGIC_BYTES)) {
            first = in.read();
            if (first != -1) {
                probed++;
            }
            second = in.read();
            if (second != -1) {
                probed++;
            }
        }
        bytesRead += probed;
        enforceBytesCap();
        failIfGzip(objectName, first, second);
        verifiedObjects.set(verified);
    }

    /**
     * Checks the gzip magic number without an extra GET, by peeking and rewinding a data stream that already begins at
     * absolute offset 0. Peeking a buffered stream is free, so this does not consult {@code verifiedObjects}; it only
     * records the result there so a later read starting mid-object can skip the paid probe.
     *
     * @param objectName the object being read
     * @param in         the buffered data stream positioned at absolute offset 0
     * @throws IOException if the peek fails or the object is gzip-compressed
     */
    private void verifyNotCompressedFromStream(String objectName, BufferedInputStream in) throws IOException {
        in.mark(GZIP_MAGIC_BYTES);
        int first = in.read();
        int second = in.read();
        in.reset();
        failIfGzip(objectName, first, second);
        verifiedObjects.set(sliceIndexForObject(objectName));
    }

    private void failIfGzip(String objectName, int first, int second) throws IOException {
        if (first == GZIP_MAGIC_0 && second == GZIP_MAGIC_1) {
            throw new IOException(
                "object ["
                    + LogSanitizer.sanitize(objectName)
                    + "] appears to be gzip-compressed (magic bytes 1f 8b); the object_store source requires uncompressed ndjson in v0"
            );
        }
    }

    private void enforceGetRequestCap() throws CostLimitExceededException {
        if (getRequestCount > maxGetRequests) {
            throw new CostLimitExceededException(
                "get request count [" + getRequestCount + "] exceeds max_get_requests [" + maxGetRequests + "]"
            );
        }
    }

    /**
     * Returns how many more bytes may be read before {@code max_bytes} is exceeded, saturating when the cap is
     * unlimited. Handed to the counting stream so an oversized slice is abandoned in flight instead of being fully
     * downloaded and framed before the cap is checked.
     *
     * @return the remaining byte budget
     */
    private long remainingByteBudget() {
        return maxBytes == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, maxBytes - bytesRead);
    }

    private void enforceBytesCap() throws CostLimitExceededException {
        if (bytesRead > maxBytes) {
            throw new CostLimitExceededException("bytes read [" + bytesRead + "] exceeds max_bytes [" + maxBytes + "]");
        }
    }

    /**
     * A pass-through {@link InputStream} that counts the number of bytes successfully read, used to detect a
     * truncated object (a short read that delivers fewer bytes than the frozen manifest implies for the range). It
     * sits <em>above</em> the {@link BufferedInputStream} so the count tracks the bytes the framer actually consumed,
     * not the bytes the buffer prefetched, which keeps the {@code max_bytes} cap tied to consumed data. It ends the
     * stream after {@code limit} bytes, so the byte requested beyond a slice's declared end is never framed.
     */
    static final class CountingInputStream extends InputStream {
        private final InputStream delegate;
        private final long limit;
        private final long budget;
        private long count;

        /**
         * Wraps a stream.
         *
         * @param delegate the stream to count
         * @param limit    the number of bytes after which the stream reports its end
         * @param budget   the number of bytes that may still be read before {@code max_bytes} is exceeded, so the cap
         *                 stops the transfer in flight rather than after a whole slice has been downloaded and framed
         */
        CountingInputStream(InputStream delegate, long limit, long budget) {
            this.delegate = delegate;
            this.limit = limit;
            this.budget = budget;
        }

        private void enforceBudget() throws CostLimitExceededException {
            if (count > budget) {
                throw new CostLimitExceededException("bytes read exceeds max_bytes while reading; the read was stopped in flight");
            }
        }

        @Override
        public int read() throws IOException {
            if (count >= limit) {
                return -1;
            }
            int b = delegate.read();
            if (b != -1) {
                count++;
                enforceBudget();
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (count >= limit) {
                return -1;
            }
            int n = delegate.read(b, off, (int) Math.min(len, limit - count));
            if (n > 0) {
                count += n;
                enforceBudget();
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        /**
         * Returns the number of bytes successfully read so far.
         *
         * @return the byte count
         */
        long count() {
            return count;
        }
    }
}
