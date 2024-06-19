/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * This package contains optimization for range-type aggregations
 * <p>
 * The idea is to
 * <ul>
 * <li> figure out the "ranges" from the aggregation </li>
 * <li> leverage the range filter to get the result of range bucket quickly </li>
 * </ul>
 */
package org.opensearch.search.optimization.ranges;
