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

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Tests that by default the error_trace parameter can be used to show stacktraces
 */
public class DetailedErrorsEnabledIT extends HttpSmokeTestCase {

    public void testThatErrorTraceWorksByDefault() throws IOException, ParseException {
        try {
            Request request = new Request("DELETE", "/");
            request.addParameter("error_trace", "true");
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));
            assertThat(EntityUtils.toString(response.getEntity()),
                    containsString("\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; " +
                    "nested: ActionRequestValidationException[Validation Failed: 1:"));
        }

        try {
            getRestClient().performRequest(new Request("DELETE", "/"));
            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json; charset=UTF-8"));
            assertThat(EntityUtils.toString(response.getEntity()),
                    not(containsString("\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; "
                    + "nested: ActionRequestValidationException[Validation Failed: 1:")));
        }
    }
}
