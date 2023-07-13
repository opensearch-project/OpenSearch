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

package org.opensearch;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.commons.codec.DecoderException;
import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.Optional;

import static org.opensearch.ExceptionsHelper.maybeError;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

public class ExceptionsHelperTests extends OpenSearchTestCase {

    public void testMaybeError() {
        final Error outOfMemoryError = new OutOfMemoryError();
        assertError(outOfMemoryError, outOfMemoryError);

        final DecoderException decoderException = new DecoderException(outOfMemoryError);
        assertError(decoderException, outOfMemoryError);

        final Exception e = new Exception();
        e.addSuppressed(decoderException);
        assertError(e, outOfMemoryError);

        final int depth = randomIntBetween(1, 16);
        Throwable cause = new Exception();
        boolean fatal = false;
        Error error = null;
        for (int i = 0; i < depth; i++) {
            final int length = randomIntBetween(1, 4);
            for (int j = 0; j < length; j++) {
                if (!fatal && rarely()) {
                    error = new Error();
                    cause.addSuppressed(error);
                    fatal = true;
                } else {
                    cause.addSuppressed(new Exception());
                }
            }
            if (!fatal && rarely()) {
                cause = error = new Error(cause);
                fatal = true;
            } else {
                cause = new Exception(cause);
            }
        }
        if (fatal) {
            assertError(cause, error);
        } else {
            assertFalse(maybeError(cause).isPresent());
        }

        assertFalse(maybeError(new Exception(new DecoderException())).isPresent());
    }

    private void assertError(final Throwable cause, final Error error) {
        final Optional<Error> maybeError = maybeError(cause);
        assertTrue(maybeError.isPresent());
        assertThat(maybeError.get(), equalTo(error));
    }

    public void testStatus() {
        assertThat(ExceptionsHelper.status(new IllegalArgumentException("illegal")), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ExceptionsHelper.status(new JsonParseException(null, "illegal")), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ExceptionsHelper.status(new OpenSearchRejectedExecutionException("rejected")), equalTo(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testSummaryMessage() {
        assertThat(ExceptionsHelper.summaryMessage(new IllegalArgumentException("illegal")), equalTo("Invalid argument"));
        assertThat(ExceptionsHelper.summaryMessage(new JsonParseException(null, "illegal")), equalTo("Failed to parse JSON"));
        assertThat(ExceptionsHelper.summaryMessage(new OpenSearchRejectedExecutionException("rejected")), equalTo("Too many requests"));
    }

    public void testGroupBy() {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            createShardFailureParsingException("error", "node0", "index", 0, null),
            createShardFailureParsingException("error", "node1", "index", 1, null),
            createShardFailureParsingException("error", "node2", "index2", 2, null),
            createShardFailureParsingException("error", "node0", "index", 0, "cluster1"),
            createShardFailureParsingException("error", "node1", "index", 1, "cluster1"),
            createShardFailureParsingException("error", "node2", "index", 2, "cluster1"),
            createShardFailureParsingException("error", "node0", "index", 0, "cluster2"),
            createShardFailureParsingException("error", "node1", "index", 1, "cluster2"),
            createShardFailureParsingException("error", "node2", "index", 2, "cluster2"),
            createShardFailureParsingException("another error", "node2", "index", 2, "cluster2") };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(5));
        String[] expectedIndices = new String[] { "index", "index2", "cluster1:index", "cluster2:index", "cluster2:index" };
        String[] expectedErrors = new String[] { "error", "error", "error", "error", "another error" };
        int i = 0;
        for (ShardOperationFailedException shardOperationFailedException : groupBy) {
            assertThat(shardOperationFailedException.getCause().getMessage(), equalTo(expectedErrors[i]));
            assertThat(shardOperationFailedException.index(), equalTo(expectedIndices[i++]));
        }
    }

    private static ShardSearchFailure createShardFailureParsingException(
        String error,
        String nodeId,
        String index,
        int shardId,
        String clusterAlias
    ) {
        ParsingException ex = new ParsingException(0, 0, error, new IllegalArgumentException("some bad argument"));
        ex.setIndex(index);
        return new ShardSearchFailure(ex, createSearchShardTarget(nodeId, shardId, index, clusterAlias));
    }

    private static SearchShardTarget createSearchShardTarget(String nodeId, int shardId, String index, String clusterAlias) {
        return new SearchShardTarget(
            nodeId,
            new ShardId(new Index(index, IndexMetadata.INDEX_UUID_NA_VALUE), shardId),
            clusterAlias,
            OriginalIndices.NONE
        );
    }

    public void testGroupByNullTarget() {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            createShardFailureQueryShardException("error", "index", null),
            createShardFailureQueryShardException("error", "index", null),
            createShardFailureQueryShardException("error", "index", null),
            createShardFailureQueryShardException("error", "index", "cluster1"),
            createShardFailureQueryShardException("error", "index", "cluster1"),
            createShardFailureQueryShardException("error", "index", "cluster1"),
            createShardFailureQueryShardException("error", "index", "cluster2"),
            createShardFailureQueryShardException("error", "index", "cluster2"),
            createShardFailureQueryShardException("error", "index2", null),
            createShardFailureQueryShardException("another error", "index2", null), };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(5));
        String[] expectedIndices = new String[] { "index", "cluster1:index", "cluster2:index", "index2", "index2" };
        String[] expectedErrors = new String[] { "error", "error", "error", "error", "another error" };
        int i = 0;
        for (ShardOperationFailedException shardOperationFailedException : groupBy) {
            assertThat(shardOperationFailedException.index(), nullValue());
            assertThat(shardOperationFailedException.getCause(), instanceOf(OpenSearchException.class));
            OpenSearchException openSearchException = (OpenSearchException) shardOperationFailedException.getCause();
            assertThat(openSearchException.getMessage(), equalTo(expectedErrors[i]));
            assertThat(openSearchException.getIndex().getName(), equalTo(expectedIndices[i++]));
        }
    }

    private static ShardSearchFailure createShardFailureQueryShardException(String error, String indexName, String clusterAlias) {
        Index index = new Index(RemoteClusterAware.buildRemoteIndexName(clusterAlias, indexName), "uuid");
        QueryShardException queryShardException = new QueryShardException(index, error, new IllegalArgumentException("parse error"));
        return new ShardSearchFailure(queryShardException, null);
    }

    public void testGroupByNullIndex() {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            new ShardSearchFailure(new IllegalArgumentException("error")),
            new ShardSearchFailure(new ParsingException(0, 0, "error", null)), };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(2));
    }

    public void testUnwrapCorruption() {
        final Throwable corruptIndexException = new CorruptIndexException("corrupt", "resource");
        assertThat(ExceptionsHelper.unwrapCorruption(corruptIndexException), equalTo(corruptIndexException));

        final Throwable corruptionAsCause = new RuntimeException(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionAsCause), equalTo(corruptIndexException));

        final Throwable corruptionSuppressed = new RuntimeException();
        corruptionSuppressed.addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionSuppressed), equalTo(corruptIndexException));

        final Throwable corruptionSuppressedOnCause = new RuntimeException(new RuntimeException());
        corruptionSuppressedOnCause.getCause().addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionSuppressedOnCause), equalTo(corruptIndexException));

        final Throwable corruptionCauseOnSuppressed = new RuntimeException();
        corruptionCauseOnSuppressed.addSuppressed(new RuntimeException(corruptIndexException));
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionCauseOnSuppressed), equalTo(corruptIndexException));

        assertThat(ExceptionsHelper.unwrapCorruption(new RuntimeException()), nullValue());
        assertThat(ExceptionsHelper.unwrapCorruption(new RuntimeException(new RuntimeException())), nullValue());

        final Throwable withSuppressedException = new RuntimeException();
        withSuppressedException.addSuppressed(new RuntimeException());
        assertThat(ExceptionsHelper.unwrapCorruption(withSuppressedException), nullValue());
    }

    public void testSuppressedCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException();
        e1.addSuppressed(e2);
        e2.addSuppressed(e1);
        ExceptionsHelper.unwrapCorruption(e1);

        final CorruptIndexException corruptIndexException = new CorruptIndexException("corrupt", "resource");
        RuntimeException e3 = new RuntimeException(corruptIndexException);
        e3.addSuppressed(e1);
        assertThat(ExceptionsHelper.unwrapCorruption(e3), equalTo(corruptIndexException));

        RuntimeException e4 = new RuntimeException(e1);
        e4.addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(e4), equalTo(corruptIndexException));
    }

    public void testCauseCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException(e1);
        e1.initCause(e2);
        ExceptionsHelper.unwrap(e1, IOException.class);
        ExceptionsHelper.unwrapCorruption(e1);
    }
}
