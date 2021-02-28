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
package org.elasticsearch.docker.test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

public class DockerYmlTestSuiteIT extends ESClientYamlSuiteTestCase {

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
            .append(getProperty("test.fixtures.elasticsearch-oss-1.tcp.9200"))
            .append(",")
            .append("localhost:")
            .append(getProperty("test.fixtures.elasticsearch-oss-2.tcp.9200"))
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
                "This test expects to run with the elasticsearch.test.fixtures Gradle plugin");
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

    static Path keyStore;

    @AfterClass
    public static void clearKeyStore() {
        keyStore = null;
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
