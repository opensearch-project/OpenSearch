/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Shard-fan-out stage executions: dispatch fragment work to data-node shards via Arrow Flight
 * and consume the streaming responses on the coordinator.
 */
package org.opensearch.analytics.exec.stage.shard;
