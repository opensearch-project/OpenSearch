/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadUtils;
import org.opensearch.tasks.Task;

import java.util.Arrays;

/**
 * Utility class for transport
 *
 * @opensearch.internal
 */
public enum Transports {
    ;

    /** threads whose name is prefixed by this string will be considered network threads, even though they aren't */
    public static final String TEST_MOCK_TRANSPORT_THREAD_PREFIX = "__mock_network_thread";

    /**
     * Utility method to detect whether a thread is a network thread. Typically
     * used in assertions to make sure that we do not call blocking code from
     * networking threads.
     */
    public static boolean isTransportThread(Thread t) {
        final String threadName = t.getName();
        for (String s : Arrays.asList(
            ThreadUtils.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX,
            ThreadUtils.TRANSPORT_WORKER_THREAD_NAME_PREFIX,
            TEST_MOCK_TRANSPORT_THREAD_PREFIX
        )) {
            if (threadName.contains(s)) {
                return true;
            }
        }
        return false;
    }

    public static boolean assertTransportThread() {
        final Thread t = Thread.currentThread();
        assert isTransportThread(t) : "Expected transport thread but got [" + t + "]";
        return true;
    }

    public static boolean assertNotTransportThread(String reason) {
        final Thread t = Thread.currentThread();
        assert isTransportThread(t) == false : "Expected current thread [" + t + "] to not be a transport thread. Reason: [" + reason + "]";
        return true;
    }

    public static boolean assertDefaultThreadContext(ThreadContext threadContext) {
        assert threadContext.getRequestHeadersOnly().isEmpty()
            || threadContext.getRequestHeadersOnly().size() == 1 && threadContext.getRequestHeadersOnly().containsKey(Task.X_OPAQUE_ID)
            : "expected empty context but was " + threadContext.getRequestHeadersOnly() + " on " + Thread.currentThread().getName();
        return true;
    }
}
