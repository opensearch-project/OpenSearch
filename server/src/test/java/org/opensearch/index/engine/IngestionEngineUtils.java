/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class IngestionEngineUtils {

    static class FakeIngestionConsumerFactory implements IngestionConsumerFactory<FakeIngestionConsumer, FakeIngestionShardPointer> {
        private List<byte[]> messages;

        public FakeIngestionConsumerFactory(List<byte[]> messages) {
            this.messages = messages;
        }

        @Override
        public void initialize(Map params) {
        }

        @Override
        public FakeIngestionConsumer createShardConsumer(String clientId, int shardId) {
            return new FakeIngestionConsumer(messages, shardId);
        }

        @Override
        public FakeIngestionShardPointer parsePointerFromString(String pointer) {
            return new FakeIngestionShardPointer(Long.valueOf(pointer));
        }
    }

    static class FakeIngestionConsumer implements IngestionShardConsumer<FakeIngestionShardPointer, FakeIngestionMessage> {
        // FakeIngestionConsumer uses a list of byte arrays to simulate streams
        private List<byte[]> messages;
        private int shardId;
        private long lastFetchedOffset;

        FakeIngestionConsumer(List<byte[]> messages, int shardId) {
            this.messages = messages;
            this.shardId = shardId;
            this.lastFetchedOffset = -1;
        }

        @Override
        public List<ReadResult<FakeIngestionShardPointer, FakeIngestionMessage>> readNext(FakeIngestionShardPointer pointer, long maxMessages, int timeoutMillis) throws TimeoutException {
            lastFetchedOffset = pointer.offset-1;
            int numToFetch = Math.min(messages.size() - (int) pointer.offset, (int) maxMessages);
            List<ReadResult<FakeIngestionShardPointer, FakeIngestionMessage>> result = new ArrayList<>();
            for (long i = pointer.offset; i < pointer.offset + numToFetch; i++) {
                result.add(new ReadResult<>(new FakeIngestionShardPointer(i), new FakeIngestionMessage(messages.get((int) i))));
                lastFetchedOffset = i;
            }
            return result;
        }

        @Override
        public FakeIngestionShardPointer nextPointer() {
            return new FakeIngestionShardPointer(lastFetchedOffset + 1);
        }

        @Override
        public FakeIngestionShardPointer earliestPointer() {
            return new FakeIngestionShardPointer(0);
        }

        @Override
        public FakeIngestionShardPointer latestPointer() {
            return new FakeIngestionShardPointer(messages.size());
        }

        @Override
        public int getShardId() {
            return shardId;
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class FakeIngestionMessage implements Message<byte[]> {
        private final byte[] payload;

        public FakeIngestionMessage(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public byte[] getPayload() {
            return payload;
        }

        @Override
        public String toString() {
            return new String(payload);
        }
    }

    static class FakeIngestionShardPointer implements IngestionShardPointer {
        private final long offset;

        public FakeIngestionShardPointer(long offset) {
            this.offset = offset;
        }

        @Override
        public byte[] serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(offset);
            return buffer.array();
        }

        @Override
        public String asString() {
            return String.valueOf(offset);
        }

        @Override
        public String toString() {
            return asString();
        }

        @Override
        public Field asPointField(String fieldName) {
            return new LongPoint(fieldName, offset);
        }

        @Override
        public Query newRangeQueryGreaterThan(String fieldName) {
            return LongPoint.newRangeQuery(fieldName, offset, Long.MAX_VALUE);
        }

        @Override
        public int compareTo(IngestionShardPointer o) {
            FakeIngestionShardPointer other = (FakeIngestionShardPointer) o;
            return Long.compare(offset, other.offset);
        }
    }

}
