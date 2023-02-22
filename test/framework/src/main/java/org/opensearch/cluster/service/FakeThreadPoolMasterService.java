/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.threadpool.ThreadPool;

import java.util.function.Consumer;

/**
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link FakeThreadPoolClusterManagerService}
 */
@Deprecated
public class FakeThreadPoolMasterService extends FakeThreadPoolClusterManagerService {
    public FakeThreadPoolMasterService(
        String nodeName,
        String serviceName,
        ThreadPool threadPool,
        Consumer<Runnable> onTaskAvailableToRun
    ) {
        super(nodeName, serviceName, threadPool, onTaskAvailableToRun);
    }
}
