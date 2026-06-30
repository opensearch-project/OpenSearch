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

package org.opensearch.wildfly;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestRuleLimitSysouts;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@TestRuleLimitSysouts.Limit(bytes = 14000)
public class WildflyIT extends LuceneTestCase {

    private Logger logger = LogManager.getLogger(WildflyIT.class);

    private String buildBaseUrl() {
        final String propertyName = "test.fixtures.wildfly.tcp.8080";
        final String port = System.getProperty(propertyName);
        if (port == null) {
            throw new IllegalStateException(
                "Could not find system property "
                    + propertyName
                    + ". This test expects to run with the opensearch.test.fixtures Gradle plugin"
            );
        }
        return "http://localhost:" + port + "/example-app/transport";
    }

    public void testRestClient() throws URISyntaxException, IOException, ParseException {
        final String baseUrl = buildBaseUrl();

        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            final String endpoint = baseUrl + "/employees/1";
            logger.info("Connecting to uri: " + baseUrl);

            final HttpPut put = new HttpPut(new URI(endpoint));

            final String body = "{"
                + "  \"first_name\": \"John\","
                + "  \"last_name\": \"Smith\","
                + "  \"age\": 25,"
                + "  \"about\": \"I love to go rock climbing\","
                + "  \"interests\": ["
                + "    \"sports\","
                + "    \"music\""
                + "  ]"
                + "}";

            put.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = client.execute(put)) {
                int status = response.getCode();
                assertThat(
                    "expected a 201 response but got: " + status + " - body: " + EntityUtils.toString(response.getEntity()),
                    status,
                    equalTo(201)
                );
            }

            logger.info("Fetching resource at " + endpoint);

            final HttpGet get = new HttpGet(new URI(endpoint));
            try (
                CloseableHttpResponse response = client.execute(get);
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
            ) {
                final Map<String, Object> map = parser.map();
                assertThat(map.get("first_name"), equalTo("John"));
                assertThat(map.get("last_name"), equalTo("Smith"));
                assertThat(map.get("age"), equalTo(25));
                assertThat(map.get("about"), equalTo("I love to go rock climbing"));
                final Object interests = map.get("interests");
                assertThat(interests, instanceOf(List.class));
                @SuppressWarnings("unchecked")
                final List<String> interestsAsList = (List<String>) interests;
                assertThat(interestsAsList, containsInAnyOrder("sports", "music"));
            }
        }
    }

}
