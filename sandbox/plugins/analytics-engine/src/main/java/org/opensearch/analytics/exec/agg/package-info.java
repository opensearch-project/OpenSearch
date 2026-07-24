/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * MPP aggregate-shuffle dispatch orchestration. Sibling of the join package: houses the
 * post-CBO advisor that reads the {@code SHUFFLE_SCAN_AGG} stage role to route hash-shuffle
 * aggregate dispatch, the dispatcher that drives the producer-to-consumer loop, and the
 * DAG rewriter that lifts the CBO-produced "aggregate inside consumer fragment" shape into
 * a worker tier.
 */
package org.opensearch.analytics.exec.agg;
