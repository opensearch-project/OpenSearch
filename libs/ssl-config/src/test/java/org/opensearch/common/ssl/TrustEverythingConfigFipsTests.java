/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import static org.hamcrest.Matchers.containsString;

public class TrustEverythingConfigFipsTests extends TrustEverythingConfigTests {

    public void testCreateTrustManager() {
        Exception e = assertThrows(IllegalStateException.class, TrustEverythingConfig.TRUST_EVERYTHING::createTrustManager);
        assertThat(e.getMessage(), containsString("not permitted in FIPS mode"));
    }
}
