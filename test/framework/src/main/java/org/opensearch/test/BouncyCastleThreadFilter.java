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
        // Ignore BC’s global background threads
        return "BC Disposal Daemon".equals(n) || "BC Cleanup Executor".equals(n);
    }
}
