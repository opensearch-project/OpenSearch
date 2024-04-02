/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox;

import org.opensearch.action.ActionRequest;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This class is used to extract attributes from search/msearch requests at co-ordinator node level to classify the task into a Sandbox
 */
public interface RequestAttributeExtractor {
    /**
     *
     * @param request must be SearchRequest or MultiSearchRequest
     * @return
     * @throws NoSuchElementException
     */
    public Map<String, String> extractAttributesFrom(ActionRequest request) throws NoSuchElementException;
}
