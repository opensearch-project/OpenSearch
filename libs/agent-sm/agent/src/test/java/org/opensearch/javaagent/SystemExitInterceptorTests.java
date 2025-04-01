/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.Policy;
import java.util.Set;

public class SystemExitInterceptorTests {
    @SuppressWarnings("removal")
    @BeforeClass
    public static void setUp() {
        AgentPolicy.setPolicy(new Policy() {
        }, Set.of(), new String[] { "worker.org.gradle.process.internal.worker.GradleWorkerMain" });
    }

    @Test(expected = SecurityException.class)
    public void testSystemExitIsForbidden() {
        System.exit(0);
    }
}
