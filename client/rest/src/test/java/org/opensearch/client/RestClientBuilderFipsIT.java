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

public class RestClientBuilderFipsIT extends RestClientBuilderIntegTests {

    @Override
    protected SSLContext getSslContext(boolean server) throws Exception {
        return getSslContext(server, "BCFKS", SecureRandom.getInstance("DEFAULT", "BCFIPS"), ".bcfks");
    }
}
