/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Caching of BM25 statistics computed over a restricted subset of a shard.
 *
 * <p>When a request is restricted to a subset of an index, relevance statistics taken over the whole shard describe
 * documents the caller cannot see, so a term confined to those documents still lowers the inverse document frequency of
 * the documents the caller can see. Computing statistics over only the visible documents removes that, at the cost of
 * one pass over those documents per segment.
 *
 * <p>That pass is the reason this package exists. Lucene segments are immutable and a given restriction is fixed, so for
 * a (segment, restriction) pair the recomputed statistics are identical every time. Entries are therefore keyed by the
 * segment core cache key and evicted by a close listener when the segment is replaced, which means a refresh only adds
 * cold segments rather than invalidating what is already warm, and no entry can outlive the segment it describes.
 *
 * <p>Cached bytes are reported to the field data circuit breaker without breaking, so the memory is visible in node
 * statistics. Eviction is currently driven only by segment close, with no size ceiling.
 */
package org.opensearch.index.cache.filteredstats;
