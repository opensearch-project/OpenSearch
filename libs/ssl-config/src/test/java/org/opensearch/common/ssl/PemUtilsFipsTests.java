/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

public class PemUtilsFipsTests extends PemUtilsTests {

    public void testReadEncryptedPKCS8Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
    }

    public void testReadDESEncryptedPKCS1Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
    }

    public void testReadAESEncryptedPKCS1Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
    }

    public void testReadEncryptedOpenSslDsaKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
    }

    public void testReadEncryptedOpenSslEcKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
    }
}
