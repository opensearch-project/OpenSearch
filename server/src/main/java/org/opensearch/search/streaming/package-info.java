/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Streaming aggregation support for OpenSearch.
 *
 * <p>This package provides interfaces and utilities for streaming aggregations that can
 * flush results per-segment instead of per-shard, enabling faster response times for
 * large aggregations.
 *
 * @opensearch.experimental
 */
package org.opensearch.search.streaming;
