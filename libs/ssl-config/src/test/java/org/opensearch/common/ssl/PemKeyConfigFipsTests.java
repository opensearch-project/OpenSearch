/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

public class PemKeyConfigFipsTests extends PemKeyConfigTests {

    public void testBuildKeyConfigFromPkcs1PemFilesWithPassword() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
    }
}
