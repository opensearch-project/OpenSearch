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

package org.opensearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.opensearch.test.OpenSearchTestCase;

/** Simple tests system call filter is working. */
public class SystemCallFilterTests extends OpenSearchTestCase {

    /** command to try to run in tests */
    static final String[] EXECUTABLE = new String[] { Constants.WINDOWS ? "calc" : "ls" };

    @SuppressWarnings("removal")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("requires system call filter installation", Natives.isSystemCallFilterInstalled());
        // otherwise security manager will block the execution, no fun
        assumeTrue("cannot test with security manager enabled", System.getSecurityManager() == null);
        // otherwise, since we don't have TSYNC support, rules are not applied to the test thread
        // (randomizedrunner class initialization happens in its own thread, after the test thread is created)
        // instead we just forcefully run it for the test thread here.
        if (!JNANatives.LOCAL_SYSTEM_CALL_FILTER_ALL) {
            try {
                SystemCallFilter.init(createTempDir());
            } catch (Exception e) {
                throw new RuntimeException("unable to forcefully apply system call filter to test thread", e);
            }
        }
    }

    public void testNoExecution() throws Exception {
        try {
            Runtime.getRuntime().exec(EXECUTABLE);
            fail("should not have been able to execute!");
        } catch (Exception expected) {
            // we can't guarantee how its converted, currently its an IOException, like this:
            /*
            java.io.IOException: Cannot run program "ls": error=13, Permission denied
                    at __randomizedtesting.SeedInfo.seed([65E6C4BED11899E:FC6E1CA6AA2DB634]:0)
                    at java.lang.ProcessBuilder.start(ProcessBuilder.java:1048)
                    at java.lang.Runtime.exec(Runtime.java:620)
                    ...
                  Caused by: java.io.IOException: error=13, Permission denied
                    at java.lang.UNIXProcess.forkAndExec(Native Method)
                    at java.lang.UNIXProcess.<init>(UNIXProcess.java:248)
                    at java.lang.ProcessImpl.start(ProcessImpl.java:134)
                    at java.lang.ProcessBuilder.start(ProcessBuilder.java:1029)
                    ...
            */
        }
    }

    // make sure thread inherits this too (its documented that way)
    public void testNoExecutionFromThread() throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    Runtime.getRuntime().exec(EXECUTABLE);
                    fail("should not have been able to execute!");
                } catch (Exception expected) {
                    // ok
                }
            }
        };
        t.start();
        t.join();
    }
}
