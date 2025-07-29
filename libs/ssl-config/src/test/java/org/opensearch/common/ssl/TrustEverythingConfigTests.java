/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;

public class TrustEverythingConfigTests extends OpenSearchTestCase {

    public void testGetDependentFiles() {
        assertTrue(TrustEverythingConfig.TRUST_EVERYTHING.getDependentFiles().isEmpty());
    }

    public void testCreateTrustManager() {
        if (inFipsJvm()) {
            Exception e = assertThrows(IllegalStateException.class, TrustEverythingConfig.TRUST_EVERYTHING::createTrustManager);
            assertThat(e.getMessage(), containsString("not permitted in FIPS mode"));
        } else {
            var trustManager = TrustEverythingConfig.TRUST_EVERYTHING.createTrustManager();
            assertNotNull(trustManager);
        }
    }
}
