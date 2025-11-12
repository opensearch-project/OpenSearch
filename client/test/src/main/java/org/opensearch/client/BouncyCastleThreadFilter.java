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
        // Ignore BC’s global background threads
        return "BC Disposal Daemon".equals(n) || "BC Cleanup Executor".equals(n);
    }
}
