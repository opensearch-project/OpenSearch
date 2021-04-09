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

package org.opensearch.http;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CorsNotSetIT extends HttpSmokeTestCase {

    public void testCorsSettingDefaultBehaviourDoesNotReturnAnything() throws IOException {
        String corsValue = "http://localhost:9200";
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("User-Agent", "Mozilla Bar");
        options.addHeader("Origin", corsValue);
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
        assertThat(response.getHeader("Access-Control-Allow-Credentials"), nullValue());
    }

    public void testThatOmittingCorsHeaderDoesNotReturnAnything() throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
        assertThat(response.getHeader("Access-Control-Allow-Credentials"), nullValue());
    }
}
