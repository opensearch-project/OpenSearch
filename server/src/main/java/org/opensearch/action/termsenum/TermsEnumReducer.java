/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.Nullable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges the answers of all shards into one response.
 *
 * @opensearch.internal
 */
final class TermsEnumReducer {

    private TermsEnumReducer() {}

    /**
     * What one shard produced: a response or a failure, never both.
     *
     * @param shardId the shard
     * @param response the shard's response, or {@code null} when it failed
     * @param failure the shard's failure, or {@code null} when it answered
     */
    record ShardResult(ShardId shardId, @Nullable TermsEnumShardResponse response, @Nullable Exception failure) {

        ShardResult {
            if ((response == null) == (failure == null)) {
                throw new IllegalArgumentException("exactly one of response and failure must be set");
            }
        }

        static ShardResult of(ShardId shardId, TermsEnumShardResponse response) {
            return new ShardResult(shardId, response, null);
        }

        static ShardResult failed(ShardId shardId, Exception failure) {
            return new ShardResult(shardId, null, failure);
        }
    }

    private static final class Head {
        final BytesRef bytes;
        final int list;
        final int pos;

        Head(BytesRef bytes, int list, int pos) {
            this.bytes = bytes;
            this.list = list;
            this.pos = pos;
        }
    }

    /**
     * K-way merges sorted term lists in UTF-8 byte order, dropping duplicates and anything not after {@code searchAfter}.
     *
     * @param shardTerms one list per shard, each sorted by UTF-8 byte order
     * @param size the maximum number of terms to return
     * @param searchAfter only terms strictly greater than this are returned, or {@code null}
     * @return the merged terms
     */
    static List<String> mergeTerms(List<List<String>> shardTerms, int size, @Nullable String searchAfter) {
        BytesRef after = searchAfter == null ? null : new BytesRef(searchAfter);
        PriorityQueue<Head> queue = new PriorityQueue<>(Math.max(1, shardTerms.size()), (a, b) -> a.bytes.compareTo(b.bytes));
        for (int i = 0; i < shardTerms.size(); i++) {
            if (shardTerms.get(i).isEmpty() == false) {
                queue.add(new Head(new BytesRef(shardTerms.get(i).get(0)), i, 0));
            }
        }
        List<String> merged = new ArrayList<>();
        BytesRef last = null;
        while (queue.isEmpty() == false && merged.size() < size) {
            Head head = queue.poll();
            List<String> list = shardTerms.get(head.list);
            boolean skip = (after != null && head.bytes.compareTo(after) <= 0) || (last != null && head.bytes.compareTo(last) == 0);
            if (skip == false) {
                merged.add(list.get(head.pos));
                last = head.bytes;
            }
            int next = head.pos + 1;
            if (next < list.size()) {
                queue.add(new Head(new BytesRef(list.get(next)), head.list, next));
            }
        }
        return merged;
    }

    /**
     * Combines the shard results following the outcome rules.
     *
     * @param results one result per targeted shard
     * @param size the maximum number of terms to return
     * @param searchAfter only terms strictly greater than this are returned, or {@code null}
     * @return the response
     * @throws IllegalArgumentException when the field is unsupported everywhere it exists: at least one shard reported
     *         it unsupported and every other shard reported it absent or was skipped
     * @throws SearchPhaseExecutionException when every shard failed for reasons other than not answering in time
     */
    static TermsEnumResponse reduce(List<ShardResult> results, int size, @Nullable String searchAfter) {
        if (results.isEmpty()) {
            return new TermsEnumResponse(0, 0, new TermsEnumResponse.ShardFailure[0], List.of(), true);
        }
        List<List<String>> okTerms = new ArrayList<>();
        List<TermsEnumResponse.ShardFailure> failures = new ArrayList<>();
        String firstUnsupportedReason = null;
        int ok = 0;
        int absent = 0;
        int skipped = 0;
        int unsupported = 0;
        int unanswered = 0;
        boolean anyTimedOut = false;
        for (ShardResult result : results) {
            Exception cause = null;
            if (result.response() == null) {
                cause = result.failure();
                if (ExceptionsHelper.unwrapCause(cause) instanceof OpenSearchTimeoutException) {
                    unanswered++;
                }
            } else {
                TermsEnumShardResponse response = result.response();
                switch (response.outcome()) {
                    case OK:
                        ok++;
                        okTerms.add(response.terms());
                        anyTimedOut |= response.timedOut();
                        break;
                    case SKIPPED:
                        skipped++;
                        break;
                    case FIELD_ABSENT:
                        absent++;
                        break;
                    case FIELD_UNSUPPORTED:
                        unsupported++;
                        if (firstUnsupportedReason == null) {
                            firstUnsupportedReason = response.reason();
                        }
                        cause = new IllegalArgumentException(response.reason());
                        break;
                    default:
                        throw new IllegalStateException("unknown outcome [" + response.outcome() + "]");
                }
            }
            if (cause != null) {
                failures.add(new TermsEnumResponse.ShardFailure(result.shardId().getIndexName(), result.shardId().id(), cause));
            }
        }
        // a shard that failed or did not answer may hold the field with a supported type, so it rules out the 400
        if (unsupported > 0 && unsupported + absent + skipped == results.size()) {
            throw new IllegalArgumentException(firstUnsupportedReason);
        }
        if (ok + absent + skipped + unanswered == 0) {
            throw allShardsFailed(results);
        }
        return new TermsEnumResponse(
            results.size(),
            skipped,
            failures.toArray(new TermsEnumResponse.ShardFailure[0]),
            mergeTerms(okTerms, size, searchAfter),
            failures.isEmpty() && anyTimedOut == false
        );
    }

    /**
     * Builds the all-shards-failed error, with each failure tied to its shard. Failures from an unsupported field go
     * last, so the status comes from the other failures: a rejected or unavailable shard is worth retrying.
     *
     * @param results the results of a request where every shard failed or found the field unsupported
     * @return the error
     */
    private static SearchPhaseExecutionException allShardsFailed(List<ShardResult> results) {
        List<ShardSearchFailure> searchFailures = new ArrayList<>(results.size());
        List<ShardSearchFailure> unsupported = new ArrayList<>();
        for (ShardResult result : results) {
            SearchShardTarget target = new SearchShardTarget(null, result.shardId(), null, OriginalIndices.NONE);
            if (result.failure() != null) {
                searchFailures.add(new ShardSearchFailure(result.failure(), target));
            } else {
                assert result.response().outcome() == TermsEnumShardResponse.Outcome.FIELD_UNSUPPORTED : result.response().outcome();
                unsupported.add(new ShardSearchFailure(new IllegalArgumentException(result.response().reason()), target));
            }
        }
        searchFailures.addAll(unsupported);
        return new SearchPhaseExecutionException("terms_enum", "all shards failed", searchFailures.toArray(ShardSearchFailure.EMPTY_ARRAY));
    }
}
