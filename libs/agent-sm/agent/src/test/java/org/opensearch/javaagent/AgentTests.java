/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.junit.Test;

public class AgentTests extends AgentTestCase {
    @Test(expected = SecurityException.class)
    public void testSystemExitIsForbidden() {
        System.exit(0);
    }

    @Test(expected = SecurityException.class)
    public void testRuntimeHaltIsForbidden() {
        Runtime.getRuntime().halt(0);
    }
}
