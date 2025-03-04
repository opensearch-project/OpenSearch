/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class BCDisposalDaemonFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        return t.getName().equals("BC Disposal Daemon") || t.getName().equals("BC Cleanup Executor");
    }
}
