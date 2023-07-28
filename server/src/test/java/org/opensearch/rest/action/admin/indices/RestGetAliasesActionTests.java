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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.hamcrest.Matchers.equalTo;

public class RestGetAliasesActionTests extends OpenSearchTestCase {

    // # Assumes the following setup
    // curl -X PUT "localhost:9200/index" -H "Content-Type: application/json" -d'
    // {
    // "aliases": {
    // "foo": {},
    // "foobar": {}
    // }
    // }'

    public void testBareRequest() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata foobarAliasMetadata = AliasMetadata.builder("foobar").build();
        final AliasMetadata fooAliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(fooAliasMetadata, foobarAliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(false, new String[0], openMapBuilder, xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{},\"foobar\":{}}}}"));
    }

    public void testSimpleAliasWildcardMatchingNothing() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "baz*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testMultipleAliasWildcardsSomeMatching() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foobar").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "baz*", "foobar*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foobar\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeAll() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foob*", "-foo*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSome() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foo*", "-foob*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSomeAndExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final String[] aliasPattern;
        if (randomBoolean()) {
            aliasPattern = new String[] { "missing", "foo*", "-foob*" };
        } else {
            aliasPattern = new String[] { "foo*", "-foob*", "missing" };
        }

        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, aliasPattern, openMapBuilder, xContentBuilder);
        assertThat(restResponse.status(), equalTo(NOT_FOUND));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(
            restResponse.content().utf8ToString(),
            equalTo("{\"error\":\"alias [missing] missing\",\"status\":404,\"index\":{\"aliases\":{\"foo\":{}}}}")
        );
    }

    public void testAliasWildcardsExcludeExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foo", "foofoo", "-foo*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }
}
