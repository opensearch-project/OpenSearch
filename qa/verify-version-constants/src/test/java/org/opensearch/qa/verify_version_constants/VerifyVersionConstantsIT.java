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

package org.opensearch.qa.verify_version_constants;

import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.text.ParseException;

import static org.hamcrest.CoreMatchers.equalTo;

public class VerifyVersionConstantsIT extends OpenSearchRestTestCase {

    public void testLuceneVersionConstant() throws IOException, ParseException {
        final Response response = client().performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final String opensearchVersionString = objectPath.evaluate("version.number").toString();
        final Version opensearchVersion = Version.fromString(opensearchVersionString.replace("-SNAPSHOT", ""));
        final String luceneVersionString = objectPath.evaluate("version.lucene_version").toString();
        final org.apache.lucene.util.Version luceneVersion = org.apache.lucene.util.Version.parse(luceneVersionString);
        assertThat(opensearchVersion.luceneVersion, equalTo(luceneVersion));
    }

    @Override
    public boolean preserveClusterUponCompletion() {
        /*
         * We don't perform any writes to the cluster so there won't be anything
         * to clean up. Also, our cleanup code is really only compatible with
         * *write* compatible versions but this runs with *index* compatible
         * versions.
         */
        return true;
    }
}
