/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query.functionscore;

import java.io.IOException;

/**
 * An interface representing a term frequency function used to compute document scores
 * based on specific term frequency calculations. Implementations of this interface should
 * provide a way to execute the term frequency function for a given document ID.
 *
 * @opensearch.internal
 */
public interface TermFrequencyFunction {
    Object execute(int docId) throws IOException;
}
