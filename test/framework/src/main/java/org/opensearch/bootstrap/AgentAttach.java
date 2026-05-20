/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

public final class AgentAttach {
    public static boolean agentIsAttached() {
        try {
            Class.forName("org.opensearch.javaagent.Agent", false, ClassLoader.getSystemClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
