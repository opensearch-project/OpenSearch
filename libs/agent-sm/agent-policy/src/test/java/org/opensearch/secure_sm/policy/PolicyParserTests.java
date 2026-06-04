/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PolicyParserTests {
    private static final String POLICY = """
        grant codeBase "TestCodeBase" {
          permission java.net.NetPermission "accessUnixDomainSocket";
        };

        grant {
          permission java.net.NetPermission "accessUnixDomainSocket";
          permission java.net.SocketPermission "*", "accept,connect";
        };
        """;

    @Test
    public void testPolicy() throws IOException, PolicyParser.ParsingException {
        try (Reader reader = new StringReader(POLICY)) {
            final List<GrantEntry> grantEntries = PolicyParser.read(reader);
            assertEquals(2, grantEntries.size());

            final GrantEntry grantEntry1 = grantEntries.get(0);
            final GrantEntry grantEntry2 = grantEntries.get(1);

            assertEquals("TestCodeBase", grantEntry1.codeBase());

            List<PermissionEntry> permissions1 = grantEntry1.permissionEntries();
            assertEquals(1, permissions1.size());

            PermissionEntry firstPerm1 = permissions1.get(0);
            assertEquals("java.net.NetPermission", firstPerm1.permission());
            assertEquals("accessUnixDomainSocket", firstPerm1.name());

            assertNull(grantEntry2.codeBase());

            List<PermissionEntry> permissions2 = grantEntry2.permissionEntries();
            assertEquals(2, permissions2.size());

            PermissionEntry firstPerm2 = permissions2.get(0);
            assertEquals("java.net.NetPermission", firstPerm2.permission());
            assertEquals("accessUnixDomainSocket", firstPerm2.name());

            PermissionEntry secondPerm2 = permissions2.get(1);
            assertEquals("java.net.SocketPermission", secondPerm2.permission());
            assertEquals("*", secondPerm2.name());
            assertEquals("accept,connect", secondPerm2.action());
        }
    }
}
