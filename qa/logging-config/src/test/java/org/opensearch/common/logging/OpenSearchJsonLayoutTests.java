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


import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;


public class OpenSearchJsonLayoutTests extends OpenSearchTestCase {
    @BeforeClass
    public static void initNodeName() {
        JsonLogsTestSetup.init();
    }

    public void testEmptyType() {
        expectThrows(IllegalArgumentException.class, () -> OpenSearchJsonLayout.newBuilder().build());
    }

    public void testLayout() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
                                          .setType("serde")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:opensearch.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}\"" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testWithMaxMessageLengthLayout() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
            .setType("serde")
            .setMaxMessageLength(42)
            .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:opensearch.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-42m}{JSON}\"" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testWithUnrestrictedMaxMessageLengthLayout() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
            .setType("serde")
            .setMaxMessageLength(0)
            .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:opensearch.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%m}{JSON}\"" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testLayoutWithAdditionalFields() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
                                          .setType("serde")
                                          .setOpenSearchMessageFields("x-opaque-id,someOtherField")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:opensearch.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}\"" +
                "%notEmpty{, \"x-opaque-id\": \"%OpenSearchMessageField{x-opaque-id}\"}" +
                "%notEmpty{, \"someOtherField\": \"%OpenSearchMessageField{someOtherField}\"}" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }

    public void testLayoutWithAdditionalFieldOverride() {
        OpenSearchJsonLayout server = OpenSearchJsonLayout.newBuilder()
                                          .setType("serde")
                                          .setOpenSearchMessageFields("message")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:opensearch.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\"" +
                "%notEmpty{, \"message\": \"%OpenSearchMessageField{message}\"}" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }" + System.lineSeparator()));
    }
}
