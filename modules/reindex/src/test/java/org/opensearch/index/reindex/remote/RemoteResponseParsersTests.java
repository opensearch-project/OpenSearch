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

package org.opensearch.index.reindex.remote;

import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class RemoteResponseParsersTests extends OpenSearchTestCase {

    /**
     * Check that we can parse shard search failures without index information.
     */
    public void testFailureWithoutIndex() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new OpenSearchRejectedExecutionException("exhausted"));
        XContentBuilder builder = jsonBuilder();
        failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            ScrollableHitSource.SearchFailure parsed = RemoteResponseParsers.SEARCH_FAILURE_PARSER.parse(parser, null);
            assertNotNull(parsed.getReason());
            assertThat(parsed.getReason().getMessage(), Matchers.containsString("exhausted"));
            assertThat(parsed.getReason(), Matchers.instanceOf(OpenSearchRejectedExecutionException.class));
        }
    }
}
