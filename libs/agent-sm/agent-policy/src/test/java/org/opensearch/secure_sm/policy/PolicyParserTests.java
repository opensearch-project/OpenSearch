/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Enumeration;

public class PolicyParserTests extends OpenSearchTestCase {
    private static final String POLICY = """
        grant codeBase "TestCodeBase" {
          permission java.net.NetPermission "accessUnixDomainSocket";
        };

        grant {
          permission java.net.NetPermission "accessUnixDomainSocket";
          permission java.net.SocketPermission "*", "accept,connect";
        };
        """;

    public void testPolicy() throws IOException, PolicyParser.ParsingException {
        try (Reader reader = new StringReader(POLICY)) {
            final PolicyParser policyParser = new PolicyParser();
            policyParser.read(reader);

            final Enumeration<PolicyParser.GrantEntry> grantEntryEnumeration = policyParser.grantElements();
            final PolicyParser.GrantEntry grantEntry1 = grantEntryEnumeration.nextElement();
            final PolicyParser.GrantEntry grantEntry2 = grantEntryEnumeration.nextElement();

            assertEquals("TestCodeBase", grantEntry1.codeBase);
            assertEquals(1, grantEntry1.permissionEntries.size());
            assertEquals("java.net.NetPermission", grantEntry1.permissionEntries.getFirst().permission);
            assertEquals("accessUnixDomainSocket", grantEntry1.permissionEntries.getFirst().name);

            assertNull(grantEntry2.codeBase);
            assertEquals(2, grantEntry2.permissionEntries.size());
            assertEquals("java.net.NetPermission", grantEntry2.permissionEntries.getFirst().permission);
            assertEquals("accessUnixDomainSocket", grantEntry2.permissionEntries.getFirst().name);
            assertEquals("java.net.SocketPermission", grantEntry2.permissionEntries.getLast().permission);
            assertEquals("*", grantEntry2.permissionEntries.getLast().name);
            assertEquals("accept,connect", grantEntry2.permissionEntries.getLast().action);
        }
    }
}
