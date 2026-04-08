/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import java.util.Iterator;

/**
 * Single-pass iterator over record batches from an EngineResultStream.
 *
 * @opensearch.internal
 */
public interface EngineResultBatchIterator extends Iterator<EngineResultBatch> {
}
