/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Map;

/**
 * Provides access to commit data.
 */
@ExperimentalApi
public interface CommitData {

    /**
     * Gets the user data from the commit.
     * @return the user data map
     * @throws IOException if an I/O error occurs
     */
    Map<String, String> getUserData() throws IOException;
}
