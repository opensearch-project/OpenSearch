/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.opensearch.common.ssl.KeyStoreType;

import static org.opensearch.client.RestClientTestCase.inFipsJvm;

public interface RestClientFipsAwareTestCase {

    default void makeRequest() throws Exception {
        if (inFipsJvm()) {
            makeRequest(KeyStoreType.BCFKS);
        } else {
            makeRequest(KeyStoreType.JKS);
        }
    }

    void makeRequest(KeyStoreType keyStoreType) throws Exception;
}
