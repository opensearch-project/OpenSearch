/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Hash-shuffle worker stage executions: fan out one fragment per shuffle partition, dispatch
 * to the partition's worker node via {@code WorkerFragmentExecutionAction}, and stream
 * post-join results back through the standard Arrow Flight response path. Sibling of
 * {@code org.opensearch.analytics.exec.stage.shard} but for fragments that read from named
 * shuffle inputs rather than scanning a shard.
 */
package org.opensearch.analytics.exec.stage.worker;
