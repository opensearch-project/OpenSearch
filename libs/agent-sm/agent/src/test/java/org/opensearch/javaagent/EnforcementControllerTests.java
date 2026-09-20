/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.junit.Test;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class EnforcementControllerTests extends AgentTestCase {
    private static final Object[] FILE_ARGUMENTS = { Path.of("not-allowed"), new FileAttribute<?>[0] };
    private static final Object[] SOCKET_ARGUMENTS = { new InetSocketAddress("localhost", 9200) };

    @Test
    public void testEnforcementCanBeDisabledAndReenabled() throws Exception {
        final Method fileMethod = Files.class.getMethod("createFile", Path.class, FileAttribute[].class);
        final Method socketMethod = SocketChannel.class.getMethod("connect", SocketAddress.class);

        assertEnforced(fileMethod, socketMethod);

        assertTrue(enforcementController.setEnforcementEnabled(false));
        FileInterceptor.intercept(FILE_ARGUMENTS, fileMethod);
        SocketChannelInterceptor.intercept(SOCKET_ARGUMENTS, socketMethod);
        SystemExitInterceptor.intercept(0);
        RuntimeHaltInterceptor.intercept(0);
        assertFalse(enforcementController.setEnforcementEnabled(false));

        assertTrue(enforcementController.setEnforcementEnabled(true));
        assertEnforced(fileMethod, socketMethod);
        assertFalse(enforcementController.setEnforcementEnabled(true));
    }

    private static void assertEnforced(Method fileMethod, Method socketMethod) {
        assertThrows(SecurityException.class, () -> FileInterceptor.intercept(FILE_ARGUMENTS, fileMethod));
        assertThrows(SecurityException.class, () -> SocketChannelInterceptor.intercept(SOCKET_ARGUMENTS, socketMethod));
        assertThrows(SecurityException.class, () -> SystemExitInterceptor.intercept(0));
        assertThrows(SecurityException.class, () -> RuntimeHaltInterceptor.intercept(0));
    }
}
