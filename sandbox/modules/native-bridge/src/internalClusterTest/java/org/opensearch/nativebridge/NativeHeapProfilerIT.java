/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge;

import org.opensearch.common.settings.Settings;
import org.opensearch.nativebridge.spi.NativeHeapProfiler;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Integration test for NativeHeapProfiler JMX MBean.
 * Starts a real single-node cluster and verifies the MBean registers and validates paths.
 * Tests that require native library calls are skipped when the library is unavailable.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class NativeHeapProfilerIT extends OpenSearchIntegTestCase {

    private static final String MBEAN_NAME = "org.opensearch.native:type=HeapProfiler";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(NativeBridgeModule.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    public void testMBeanIsRegistered() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName mbean = new ObjectName(MBEAN_NAME);
        assertTrue("HeapProfiler MBean should be registered after node start", mbs.isRegistered(mbean));
    }

    public void testDumpRejectsPathTraversal() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName mbean = new ObjectName(MBEAN_NAME);

        try {
            mbs.invoke(mbean, "dump", new Object[] { "/tmp/../etc/evil.heap" }, new String[] { "java.lang.String" });
            fail("Should have rejected path with traversal");
        } catch (Exception e) {
            String msg = getRootMessage(e);
            assertTrue(msg, msg.contains("Path traversal"));
        }
    }

    public void testDumpRejectsDisallowedPath() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName mbean = new ObjectName(MBEAN_NAME);
        NativeHeapProfiler.setAllowedDumpDirs(List.of("/data/allowed"));

        try {
            mbs.invoke(mbean, "dump", new Object[] { "/etc/not_allowed.heap" }, new String[] { "java.lang.String" });
            fail("Should have rejected disallowed path");
        } catch (Exception e) {
            String msg = getRootMessage(e);
            assertTrue(msg, msg.contains("not under allowed directories"));
        }
    }

    public void testResetRejectsInvalidSample() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName mbean = new ObjectName(MBEAN_NAME);

        try {
            mbs.invoke(mbean, "reset", new Object[] { -1 }, new String[] { "int" });
            fail("Should have rejected negative lg_sample");
        } catch (Exception e) {
            String msg = getRootMessage(e);
            assertTrue(msg, msg.contains("must be between"));
        }
    }

    private static String getRootMessage(Exception e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() != null ? cause.getMessage() : cause.toString();
    }
}
