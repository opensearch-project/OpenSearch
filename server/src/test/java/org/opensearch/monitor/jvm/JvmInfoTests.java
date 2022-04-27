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

package org.opensearch.monitor.jvm;

import org.apache.lucene.util.Constants;
import org.opensearch.test.OpenSearchTestCase;

public class JvmInfoTests extends OpenSearchTestCase {

    public void testUseG1GC() {
        // if we are running on HotSpot, and the test JVM was started
        // with UseG1GC, then JvmInfo should successfully report that
        // G1GC is enabled
        if (Constants.JVM_NAME.contains("HotSpot") || Constants.JVM_NAME.contains("OpenJDK")) {
            assertEquals(Boolean.toString(isG1GCEnabled()), JvmInfo.jvmInfo().useG1GC());
        } else {
            assertEquals("unknown", JvmInfo.jvmInfo().useG1GC());
        }
    }

    private boolean isG1GCEnabled() {
        final String argline = System.getProperty("tests.jvm.argline");
        final boolean g1GCEnabled = flagIsEnabled(argline, "UseG1GC");
        // for JDK 9 the default collector when no collector is specified is G1 GC
        final boolean noOtherCollectorSpecified = argline == null
            || (!flagIsEnabled(argline, "UseParNewGC")
                && !flagIsEnabled(argline, "UseParallelGC")
                && !flagIsEnabled(argline, "UseParallelOldGC")
                && !flagIsEnabled(argline, "UseSerialGC")
                && !flagIsEnabled(argline, "UseConcMarkSweepGC"));
        return g1GCEnabled || noOtherCollectorSpecified;
    }

    private boolean flagIsEnabled(String argline, String flag) {
        final boolean containsPositiveFlag = argline != null && argline.contains("-XX:+" + flag);
        if (!containsPositiveFlag) return false;
        final int index = argline.lastIndexOf(flag);
        return argline.charAt(index - 1) == '+';
    }
}
