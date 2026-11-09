/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.keystore;

public class AddFileKeyStoreCommandFipsTests extends AddFileKeyStoreCommandTests {

    protected String getPassword() {
        return "keystorepassword";
    }

    public void testMissingCreateWithEmptyPasswordWhenPrompted() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
    }

    public void testMissingCreateWithEmptyPasswordWithoutPromptIfForced() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
    }

    public void testAddToUnprotectedKeystore() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
    }
}
