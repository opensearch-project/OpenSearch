/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.keystore;

import java.security.NoSuchProviderException;

import static org.hamcrest.Matchers.containsString;

public class KeyStoreWrapperFipsTests extends KeyStoreWrapperTests {

    protected char[] getPassword() {
        return "6!6428DQXwPpi7@$ggeg/=".toCharArray();
    }

    public void testFailLoadV1KeystoresInFipsJvm() throws Exception {
        Exception e = assertThrows(NoSuchProviderException.class, super::generateV1);
        assertThat(e.getMessage(), containsString("no such provider: SunJCE"));
    }

    public void testFailLoadV2KeystoresInFipsJvm() throws Exception {
        Exception e = assertThrows(NoSuchProviderException.class, super::generateV2);
        assertThat(e.getMessage(), containsString("no such provider: SunJCE"));
    }

    public void testBackcompatV1() throws Exception {
        assumeFalse("Can't run in a FIPS JVM as PBE is not available", inFipsJvm());
    }

    public void testBackcompatV2() throws Exception {
        assumeFalse("Can't run in a FIPS JVM as PBE is not available", inFipsJvm());
    }
}
