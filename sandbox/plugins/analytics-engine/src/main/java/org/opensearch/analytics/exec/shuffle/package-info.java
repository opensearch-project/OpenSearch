/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Hash-shuffle transport scaffolding for the M2 join path: per-partition
 * buffering and exponential-backoff retry for coordinator↔data-node shuffle
 * data transfer. Not yet wired end-to-end pending the M2 split-rule redesign.
 */
package org.opensearch.analytics.exec.shuffle;
