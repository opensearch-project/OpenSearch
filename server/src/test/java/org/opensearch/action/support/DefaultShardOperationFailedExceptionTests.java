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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.Index;
import org.opensearch.index.shard.ShardId;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DefaultShardOperationFailedExceptionTests extends OpenSearchTestCase {

    public void testToString() {
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                new OpenSearchException("foo", new IllegalArgumentException("bar", new RuntimeException("baz")))
            );
            assertEquals(
                "[null][-1] failed, reason [OpenSearchException[foo]; nested: "
                    + "IllegalArgumentException[bar]; nested: RuntimeException[baz]; ]",
                exception.toString()
            );
        }
        {
            OpenSearchException openSearchException = new OpenSearchException("foo");
            openSearchException.setIndex(new Index("index1", "_na_"));
            openSearchException.setShard(new ShardId("index1", "_na_", 1));
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(openSearchException);
            assertEquals("[index1][1] failed, reason [OpenSearchException[foo]]", exception.toString());
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException("index2", 2, new Exception("foo"));
            assertEquals("[index2][2] failed, reason [Exception[foo]]", exception.toString());
        }
    }

    public void testToXContent() throws IOException {
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(new OpenSearchException("foo"));
            assertEquals(
                "{\"shard\":-1,\"index\":null,\"status\":\"INTERNAL_SERVER_ERROR\","
                    + "\"reason\":{\"type\":\"exception\",\"reason\":\"foo\"}}",
                Strings.toString(XContentType.JSON, exception)
            );
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                new OpenSearchException("foo", new IllegalArgumentException("bar"))
            );
            assertEquals(
                "{\"shard\":-1,\"index\":null,\"status\":\"INTERNAL_SERVER_ERROR\",\"reason\":{\"type\":\"exception\","
                    + "\"reason\":\"foo\",\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"bar\"}}}",
                Strings.toString(XContentType.JSON, exception)
            );
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                new BroadcastShardOperationFailedException(new ShardId("test", "_uuid", 2), "foo", new IllegalStateException("bar"))
            );
            assertEquals(
                "{\"shard\":2,\"index\":\"test\",\"status\":\"INTERNAL_SERVER_ERROR\","
                    + "\"reason\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}}",
                Strings.toString(XContentType.JSON, exception)
            );
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                "test",
                1,
                new IllegalArgumentException("foo")
            );
            assertEquals(
                "{\"shard\":1,\"index\":\"test\",\"status\":\"BAD_REQUEST\","
                    + "\"reason\":{\"type\":\"illegal_argument_exception\",\"reason\":\"foo\"}}",
                Strings.toString(XContentType.JSON, exception)
            );
        }
    }

    public void testFromXContent() throws IOException {
        XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent)
            .startObject()
            .field("shard", 1)
            .field("index", "test")
            .field("status", "INTERNAL_SERVER_ERROR")
            .startObject("reason")
            .field("type", "exception")
            .field("reason", "foo")
            .endObject()
            .endObject();
        builder = shuffleXContent(builder);
        DefaultShardOperationFailedException parsed;
        try (XContentParser parser = createParser(xContent, BytesReference.bytes(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = DefaultShardOperationFailedException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.shardId(), 1);
        assertEquals(parsed.index(), "test");
        assertEquals(parsed.status(), RestStatus.INTERNAL_SERVER_ERROR);
        assertEquals(parsed.getCause().getMessage(), "OpenSearch exception [type=exception, reason=foo]");
    }

    public void testSerialization() throws Exception {
        final DefaultShardOperationFailedException exception = randomInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            exception.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                final DefaultShardOperationFailedException deserializedException = new DefaultShardOperationFailedException(in);
                assertNotSame(exception, deserializedException);
                assertThat(deserializedException.index(), equalTo(exception.index()));
                assertThat(deserializedException.shardId(), equalTo(exception.shardId()));
                assertThat(deserializedException.reason(), equalTo(exception.reason()));
                assertThat(deserializedException.getCause().getMessage(), equalTo(exception.getCause().getMessage()));
                assertThat(deserializedException.getCause().getClass(), equalTo(exception.getCause().getClass()));
                assertArrayEquals(deserializedException.getCause().getStackTrace(), exception.getCause().getStackTrace());
            }
        }
    }

    private static DefaultShardOperationFailedException randomInstance() {
        final Exception cause = randomException();
        if (cause instanceof OpenSearchException) {
            return new DefaultShardOperationFailedException((OpenSearchException) cause);
        } else {
            return new DefaultShardOperationFailedException(randomAlphaOfLengthBetween(1, 5), randomIntBetween(0, 10), cause);
        }
    }

    @SuppressWarnings("unchecked")
    private static Exception randomException() {
        Supplier<Exception> supplier = randomFrom(
            () -> new CorruptIndexException(randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new NullPointerException(randomAlphaOfLengthBetween(1, 5)),
            () -> new NumberFormatException(randomAlphaOfLengthBetween(1, 5)),
            () -> new IllegalArgumentException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new AlreadyClosedException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new EOFException(randomAlphaOfLengthBetween(1, 5)),
            () -> new SecurityException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new StringIndexOutOfBoundsException(randomAlphaOfLengthBetween(1, 5)),
            () -> new ArrayIndexOutOfBoundsException(randomAlphaOfLengthBetween(1, 5)),
            () -> new StringIndexOutOfBoundsException(randomAlphaOfLengthBetween(1, 5)),
            () -> new FileNotFoundException(randomAlphaOfLengthBetween(1, 5)),
            () -> new IllegalStateException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new LockObtainFailedException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new InterruptedException(randomAlphaOfLengthBetween(1, 5)),
            () -> new IOException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new OpenSearchRejectedExecutionException(randomAlphaOfLengthBetween(1, 5), randomBoolean()),
            () -> new IndexFormatTooNewException(randomAlphaOfLengthBetween(1, 10), randomInt(), randomInt(), randomInt()),
            () -> new IndexFormatTooOldException(randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5))
        );
        return supplier.get();
    }

    private static Exception randomExceptionOrNull() {
        return randomBoolean() ? randomException() : null;
    }
}
