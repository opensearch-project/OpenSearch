/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.pipeline.ProcessorExecutionDetail.PROCESSOR_EXECUTION_DETAILS_KEY;

/**
 * A holder for state that is passed through each processor in the pipeline.
 */
public class PipelineProcessingContext {
    private final Map<String, Object> attributes = new HashMap<>();

    /**
     * Set a generic attribute in the state for this request. Overwrites any existing value.
     *
     * @param name the name of the attribute to set
     * @param value the value to set on the attributen
     */
    public void setAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    /**
     * Retrieves a generic attribute value from the state for this request.
     * @param name the name of the attribute
     * @return the value of the attribute if previously set (and null otherwise)
     */
    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    /**
     * Add a ProcessorExecutionDetail to the list of execution details.
     *
     * @param detail the ProcessorExecutionDetail to add
     */
    public void addProcessorExecutionDetail(ProcessorExecutionDetail detail) {
        @SuppressWarnings("unchecked")
        List<ProcessorExecutionDetail> details = (List<ProcessorExecutionDetail>) attributes.computeIfAbsent(
            PROCESSOR_EXECUTION_DETAILS_KEY,
            k -> new ArrayList<>()
        );
        details.add(detail);
    }

    /**
     * Get all ProcessorExecutionDetails recorded in this context.
     *
     * @return an unmodifiable list of ProcessorExecutionDetails
     */
    @SuppressWarnings("unchecked")
    public List<ProcessorExecutionDetail> getProcessorExecutionDetails() {
        Object details = attributes.get(PROCESSOR_EXECUTION_DETAILS_KEY);
        if (details instanceof List) {
            return Collections.unmodifiableList((List<ProcessorExecutionDetail>) details);
        }
        return Collections.emptyList();
    }

}
