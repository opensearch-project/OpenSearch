/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.Closeable;

/**
 * Engine-agnostic search execution context.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchExecutionContext extends Closeable {

    ShardSearchRequest request();

    SearchShardTarget shardTarget();
}
