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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.logging;

import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import static org.opensearch.common.logging.TestThreadInfoPatternConverter.threadInfo;

public class TestThreadInfoPatternConverterTests extends OpenSearchTestCase {
    private static String suiteInfo;

    @BeforeClass
    public static void captureSuiteInfo() {
        suiteInfo = threadInfo(Thread.currentThread().getName());
    }

    public void testThreadInfo() {
        // Threads that are part of a node get the node name
        String nodeName = randomAlphaOfLength(5);
        String threadPool = randomAlphaOfLength(20);
        String threadNumber = "T#" + between(0, 1000);
        String threadName = OpenSearchExecutors.threadName(nodeName, threadPool + "][" + threadNumber + "]");
        assertEquals(nodeName + " " + threadPool + " " + threadNumber, threadInfo(threadName));

        // Test threads get the test name
        assertEquals(getTestName(), threadInfo(Thread.currentThread().getName()));

        // Suite initialization gets "suite"
        assertEquals("suite", suiteInfo);

        // And stuff that doesn't match anything gets wrapped in [] so we can see it
        String unmatched = randomAlphaOfLength(5);
        assertEquals("[" + unmatched + "]", threadInfo(unmatched));
    }
}
