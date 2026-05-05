/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.security.Security;

public class RandomnessTests extends OpenSearchTestCase {

    private static final String originalStrongAlgos = Security.getProperty("securerandom.strongAlgorithms");
    protected static final String SUN = "SUN";

    @After
    void restore() throws Exception {
        if (originalStrongAlgos != null) {
            Security.setProperty("securerandom.strongAlgorithms", originalStrongAlgos);
        }
    }

    public void testCreateSecure() {
        assertEquals(SUN, Randomness.createSecure().getProvider().getName());
    }

}
