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

package org.opensearch.threadpool;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class ThreadPoolStatsTests extends OpenSearchTestCase {
    public void testThreadPoolStatsSort() throws IOException {
        List<ThreadPoolStats.Stats> stats = new ArrayList<>();
        stats.add(new ThreadPoolStats.Stats("z", -1, 0, 0, 0, 0, 0L, 0L));
        stats.add(new ThreadPoolStats.Stats("m", 3, 0, 0, 0, 0, 0L, 0L));
        stats.add(new ThreadPoolStats.Stats("m", 1, 0, 0, 0, 0, 0L, 0L));
        stats.add(new ThreadPoolStats.Stats("d", -1, 0, 0, 0, 0, 0L, 0L));
        stats.add(new ThreadPoolStats.Stats("m", 2, 0, 0, 0, 0, 0L, 0L));
        stats.add(new ThreadPoolStats.Stats("t", -1, 0, 0, 0, 0, 0L, 0L));
        stats.add(new ThreadPoolStats.Stats("a", -1, 0, 0, 0, 0, 0L, 0L));

        List<ThreadPoolStats.Stats> copy = new ArrayList<>(stats);
        Collections.sort(copy);

        List<String> names = new ArrayList<>(copy.size());
        for (ThreadPoolStats.Stats stat : copy) {
            names.add(stat.getName());
        }
        assertThat(names, contains("a", "d", "m", "m", "m", "t", "z"));

        List<Integer> threads = new ArrayList<>(copy.size());
        for (ThreadPoolStats.Stats stat : copy) {
            threads.add(stat.getThreads());
        }
        assertThat(threads, contains(-1, -1, 1, 2, 3, -1, -1));
    }

    public void testThreadPoolStatsToXContent() throws IOException {
        try (BytesStreamOutput os = new BytesStreamOutput()) {

            List<ThreadPoolStats.Stats> stats = new ArrayList<>();
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.SEARCH, -1, 0, 0, 0, 0, 0L, 0L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.WARMER, -1, 0, 0, 0, 0, 0L, -1L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.GENERIC, -1, 0, 0, 0, 0, 0L, -1L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.REPLICATION, -1, 0, 0, 0, 0, 0L, -1L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.FORCE_MERGE, -1, 0, 0, 0, 0, 0L, -1L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.SAME, -1, 0, 0, 0, 0, 0L, -1L));

            ThreadPoolStats threadPoolStats = new ThreadPoolStats(stats);
            try (XContentBuilder builder = new XContentBuilder(MediaTypeRegistry.JSON.xContent(), os)) {
                builder.startObject();
                threadPoolStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
            }

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, os.bytes())) {
                XContentParser.Token token = parser.currentToken();
                assertNull(token);

                token = parser.nextToken();
                assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                token = parser.nextToken();
                assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.currentName(), equalTo(ThreadPoolStats.Fields.THREAD_POOL));

                token = parser.nextToken();
                assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                token = parser.nextToken();
                assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));

                List<String> names = new ArrayList<>();
                while (token == XContentParser.Token.FIELD_NAME) {
                    names.add(parser.currentName());

                    token = parser.nextToken();
                    assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                    parser.skipChildren();
                    token = parser.nextToken();
                }
                assertThat(
                    names,
                    contains(
                        ThreadPool.Names.FORCE_MERGE,
                        ThreadPool.Names.GENERIC,
                        ThreadPool.Names.REPLICATION,
                        ThreadPool.Names.SAME,
                        ThreadPool.Names.SEARCH,
                        ThreadPool.Names.WARMER
                    )
                );
            }
        }
    }
}
