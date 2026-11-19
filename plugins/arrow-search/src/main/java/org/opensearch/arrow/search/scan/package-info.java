/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Shard scans of Arrow searches. Each shard stream runs one query phase on a data node, whose collectors read doc values
 * into Arrow batches and send each batch to the coordinator as soon as it is full.
 */
package org.opensearch.arrow.search.scan;
