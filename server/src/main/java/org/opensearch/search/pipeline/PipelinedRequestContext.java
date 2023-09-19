/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * A holder for state that is passed through each processor in the pipeline.
 */
public class PipelinedRequestContext {
    private final Map<String, Object> genericRequestContext = new HashMap<>();

    public Map<String, Object> getGenericRequestContext() {
        return genericRequestContext;
    }
}
