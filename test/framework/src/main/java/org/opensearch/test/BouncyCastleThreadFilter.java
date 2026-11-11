/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * ThreadFilter to exclude ThreadLeak checks for BC’s global background threads
 */
public class BouncyCastleThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        String n = t.getName();
        // Ignore BC’s global background threads. The entropy daemon starts with the first BCFIPS DRBG: before any suite in a FIPS
        // JVM, but inside the first suite that uses BCFIPS when it is only declared after the JDK's providers.
        return "BC Disposal Daemon".equals(n) || "BC Cleanup Executor".equals(n) || "BC FIPS Entropy Daemon".equals(n);
    }
}
