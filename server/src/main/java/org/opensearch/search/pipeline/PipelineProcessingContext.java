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

/**
 * A holder for state that is passed through each processor in the pipeline.
 */
public class PipelineProcessingContext {
    private final Map<String, Object> attributes = new HashMap<>();
    private final List<ProcessorExecutionDetail> processorExecutionDetails = new ArrayList<>();

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
        processorExecutionDetails.add(detail);
    }

    /**
     * Get all ProcessorExecutionDetails recorded in this context.
     *
     * @return a list of ProcessorExecutionDetails
     */
    public List<ProcessorExecutionDetail> getProcessorExecutionDetails() {
        return Collections.unmodifiableList(processorExecutionDetails);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }
}
