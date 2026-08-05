/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Sibling pipeline aggregation support: buckets_path resolution, gap policy handling,
 * and composition of second-level aggregate plans over a sibling aggregation's
 * visible buckets.
 */
package org.opensearch.dsl.aggregation.pipeline;
