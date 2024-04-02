/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox;

import org.opensearch.action.ActionRequest;

/**
 * This class is used to classify co-ordinator search reqyests into  sandboxes
 */
public class RequestSandboxClassifier {

    /**
     *
     * @param request is a coordinator request task
     * @return List of matching sandboxes based on user firing the request
     */
    public String resolveSandboxFor(final ActionRequest request) {
        return "dummy";
    }

}
