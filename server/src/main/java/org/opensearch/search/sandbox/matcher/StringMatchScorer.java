/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox.matcher;

/**
 * This interface is mainly for calculating the match score of two strings
 */
public interface StringMatchScorer {
    public double score(String input, String target) throws IllegalArgumentException;
}
