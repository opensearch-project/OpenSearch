/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * ThreadFilter to exclude ThreadLeak checks for BC’s global background threads
 *
 * <p>clone from the original, which is located in ':test:framework'</p>
 */
public class BouncyCastleThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        String n = t.getName();
        // Ignore BC’s global background threads: the whole JVM shares them, and they outlive the test that started them.
        // "BC FIPS Entropy Daemon" is started by the BouncyCastle FIPS provider when the JDK creates it ("C:HYBRID" in java.security).
        // The JDK does that on first use, which can be inside a test, for example when TLS looks up an algorithm that none of the
        // JDK's own providers offers.
        return "BC Disposal Daemon".equals(n) || "BC Cleanup Executor".equals(n) || "BC FIPS Entropy Daemon".equals(n);
    }
}
