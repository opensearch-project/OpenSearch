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

package org.opensearch.common.logging;


import org.junit.BeforeClass;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;

public class OpenSearchJsonLayoutTests extends OpenSearchTestCase {
    @BeforeClass
    public static void initNodeName() {
        JsonLogsTestSetup.init();
    }

    public void testEmptyType() {
        expectThrows(IllegalArgumentException.class, () -> OpenSearchJsonLayout.newBuilder().build());
    }

    public void testDefaultLayout() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
                                          .setType("server")
                                          .build();
        String configuration = server.toString();

        assertThat(configuration, containsString("type=server"));
        assertThat(configuration, containsString("compact=false"));
        assertThat(configuration, containsString("complete=false"));
        assertThat(configuration, containsString("eol=\\r\\n"));
        assertThat(configuration, containsString("headerPattern=[\\r\\n"));
        assertThat(configuration, containsString("footerPattern=]\\r\\n"));
        assertThat(configuration, containsString("stacktraceAsString=false"));
        assertThat(configuration, containsString("timestamp=%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}"));
        assertThat(configuration, containsString("level=%p"));
        assertThat(configuration, containsString("component=%c{1.}"));
        assertThat(configuration, containsString("cluster.name=${sys:opensearch.logs.cluster_name}"));
        assertThat(configuration, containsString("node.name=%node_name"));
        assertThat(configuration, containsString("message=%notEmpty{%marker{JSON} }%m{%.-10000m}{JSON}"));
        assertThat(configuration, containsString("cluster.uuid=%node_and_cluster_id{cluster_uuid}"));
        assertThat(configuration, containsString("node.id=%node_and_cluster_id{node_id}"));
    }

    public void testLayoutWithAdditionalFields() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
                                          .setType("server")
                                          .setOpenSearchMessageFields("x-opaque-id,someOtherField")
                                          .build();

        String configuration = server.toString();

        assertThat(configuration, containsString("x-opaque-id=%OpenSearchMessageField{x-opaque-id}"));
        assertThat(configuration, containsString("someOtherField=%OpenSearchMessageField{someOtherField}"));
    }

    public void testLayoutWithAdditionalFieldOverride() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
                                          .setType("server")
                                          .setOpenSearchMessageFields("message")
                                          .build();
        String configuration = server.toString();

        assertThat(configuration, containsString("message=%OpenSearchMessageField{message}"));
    }

    public void testLayoutWithRemovedField() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
            .setType("server")
            .setAdditionalFields("timestamp=,additionalLevel=%p")
            .build();
        String configuration = server.toString();

        assertThat(configuration, not(containsString("timestamp=")));
        assertThat(configuration, containsString("additionalLevel=%p"));
    }

    public void testLayoutNonDefaultSettings() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
            .setType("server")
            .setComplete(true)
            .setCompact(true)
            .setEventEol(true)
            .setEndOfLine("\r\n\r\n")
            .setStacktraceAsString(true)
            .build();
        String configuration = server.toString();

        assertThat(configuration, containsString("complete=true"));
        assertThat(configuration, containsString("compact=true"));
        assertThat(configuration, containsString("stacktraceAsString=true"));
        assertThat(configuration, containsString("eol=\\r\\n\\r\\n"));
    }

    public void testLayoutEolWithCompact() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
            .setType("server")
            .setCompact(true)
            .setEventEol(false)
            .build();
        String configuration = server.toString();

        assertThat(configuration, containsString("compact=true"));
        assertThat(configuration, containsString("eol=,"));
    }

    public void testLayoutCustomHeaderAndFooterPatterns() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
            .setType("server")
            .setHeaderPattern("var logs = [")
            .setFooterPattern("];")
            .build();
        String configuration = server.toString();

        assertThat(configuration, containsString("headerPattern=var logs = ["));
        assertThat(configuration, containsString("footerPattern=];"));
    }


}
