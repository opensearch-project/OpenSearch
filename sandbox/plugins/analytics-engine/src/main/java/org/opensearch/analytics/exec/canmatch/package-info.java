/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Can-match pre-filter phase for the analytics engine.
 *
 * <p>Before dispatching fragment execution to data-node shards, the coordinator
 * checks parquet row-group metadata statistics against the query's range
 * predicates. Shards where no row group can match are eliminated, avoiding
 * expensive full-scan execution.
 *
 * @opensearch.internal
 */
package org.opensearch.analytics.exec.canmatch;
