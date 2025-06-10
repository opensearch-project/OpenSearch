/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import static org.opensearch.client.RestClientTestCase.inFipsJvm;

public interface RestClientFipsAwareTestCase {

    default void makeRequest() throws Exception {
        if (inFipsJvm()) {
            makeRequest("BCFKS");
        } else {
            makeRequest("JKS");
        }
    }

    void makeRequest(String keyStoreType) throws Exception;
}
