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

package org.opensearch.docker.test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.rest.yaml.ClientYamlTestCandidate;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;

import java.io.IOException;

public class DockerYmlTestSuiteIT extends OpenSearchClientYamlSuiteTestCase {

    public DockerYmlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return new StringBuilder()
            .append("localhost:")
            .append(getProperty("test.fixtures.opensearch-1.tcp.9200"))
            .append(",")
            .append("localhost:")
            .append(getProperty("test.fixtures.opensearch-2.tcp.9200"))
            .toString();
    }

    @Override
    protected boolean randomizeContentType() {
        return false;
    }

    private String getProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("Could not find system properties from test.fixtures. " +
                "This test expects to run with the opensearch.test.fixtures Gradle plugin");
        }
        return value;
    }

    @Before
    public void waitForCluster() throws IOException {
        super.initClient();
        Request health = new Request("GET", "/_cluster/health");
        health.addParameter("wait_for_nodes", "2");
        health.addParameter("wait_for_status", "yellow");
        client().performRequest(health);
    }

    @Override
    protected Settings restClientSettings() {
        return super.restClientSettings();
    }

    @Override
    protected String getProtocol() {
        return "http";
    }
}
