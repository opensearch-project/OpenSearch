/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import javax.net.ssl.SSLContext;

import java.security.SecureRandom;

import static org.opensearch.client.RestClientTestCase.inFipsJvm;

public interface RestClientFipsAwareTestCase {

    default SSLContext getSslContext(boolean server) throws Exception {
        String keyStoreType = inFipsJvm() ? "BCFKS" : "JKS";
        String fileExtension = inFipsJvm() ? ".bcfks" : ".jks";
        SecureRandom secureRandom = inFipsJvm() ? SecureRandom.getInstance("DEFAULT", "BCFIPS") : new SecureRandom();

        return getSslContext(server, keyStoreType, secureRandom, fileExtension);
    }

    SSLContext getSslContext(boolean server, String keyStoreType, SecureRandom secureRandom, String fileExtension) throws Exception;
}
