/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import org.opensearch.test.OpenSearchTestCase;

public class TrustEverythingConfigTests extends OpenSearchTestCase {

    public void testGetDependentFiles() {
        assertTrue(TrustEverythingConfig.TRUST_EVERYTHING.getDependentFiles().isEmpty());
    }

    public void testCreateTrustManager() {
        var trustManager = TrustEverythingConfig.TRUST_EVERYTHING.createTrustManager();
        assertNotNull(trustManager);
    }
}
