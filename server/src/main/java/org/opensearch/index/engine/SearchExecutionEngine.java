/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

/**
 * SearchExecutionEngine
 * @opensearch.internal
 */
@ExperimentalApi
public interface SearchExecutionEngine {
    /**
     * execute
     * @param queryPlanIR
     * @return
     */
    Map<String, Object[]> execute(byte[] queryPlanIR);
}
