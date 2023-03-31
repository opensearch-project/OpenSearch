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

package org.opensearch.core.internal.net;

import org.apache.lucene.util.Constants;
import org.opensearch.common.util.net.NetUtils;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.Runtime.Version;

public class NetUtilsTests extends OpenSearchTestCase {

    public void testExtendedSocketOptions() {
        assumeTrue("JDK possibly not supported", Constants.JVM_NAME.contains("HotSpot") || Constants.JVM_NAME.contains("OpenJDK"));
        assumeTrue("JDK version not supported", Runtime.version().compareTo(Version.parse("11")) >= 0);
        assumeTrue("Platform possibly not supported", IOUtils.LINUX || IOUtils.MAC_OS_X);
        assertNotNull(NetUtils.getTcpKeepIdleSocketOptionOrNull());
        assertNotNull(NetUtils.getTcpKeepIntervalSocketOptionOrNull());
        assertNotNull(NetUtils.getTcpKeepCountSocketOptionOrNull());
    }
}
