/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox.matcher;

import java.util.Map;

/**
 * Main interface for calculating the score of a sandbox and request match
 */
public interface SandboxMatchScorer {
    public double score(Map<String, String> requestAttributes, Map<String, String> sandboxAttributes) throws IllegalArgumentException;
}
