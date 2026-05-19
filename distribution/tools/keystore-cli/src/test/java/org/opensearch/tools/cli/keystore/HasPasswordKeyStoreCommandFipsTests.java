/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.keystore;

public class HasPasswordKeyStoreCommandFipsTests extends HasPasswordKeyStoreCommandTests {

    public void testFailsWhenKeystoreLacksPassword() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
    }
}
