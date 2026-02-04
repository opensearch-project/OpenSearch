/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Interface representing the intended workload optimization a user wants their k-NN system to have. 
 * Based on this value, default parameter resolution will be determined.
 */
public interface Mode {
    public String getName();
}
