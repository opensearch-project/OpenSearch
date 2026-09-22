/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Coordinator-side stage executions and their factories: local pass-through, local compute,
 * and reduce stages that run in-process on the coordinator (no shard dispatch).
 */
package org.opensearch.analytics.exec.stage.coordinator;
