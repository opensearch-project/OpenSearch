/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.search.aggregations.bucket.SingleBucketAggregation;

/**
 * A single bucket holding a uniform random sample of the documents matching the query, with counts underneath it
 * scaled back up to estimates for the whole match set.
 * <p>
 * Unlike {@link Sampler}, which keeps the top-scoring documents of each shard, this one ignores scores entirely.
 *
 * @opensearch.internal
 */
public interface RandomSampler extends SingleBucketAggregation {}
