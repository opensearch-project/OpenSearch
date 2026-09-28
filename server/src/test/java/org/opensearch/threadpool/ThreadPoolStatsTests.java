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

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
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
        ThreadPoolStats.Stats.Builder defaultStats = new ThreadPoolStats.Stats.Builder().queue(0)
            .active(0)
            .rejected(0)
            .largest(0)
            .completed(0L)
            .waitTimeNanos(0L)
            .parallelism(-1);
        stats.add(defaultStats.name("z").threads(-1).build());
        stats.add(defaultStats.name("m").threads(3).build());
        stats.add(defaultStats.name("m").threads(1).build());
        stats.add(defaultStats.name("d").threads(-1).build());
        stats.add(defaultStats.name("m").threads(2).build());
        stats.add(defaultStats.name("t").threads(-1).build());
        stats.add(defaultStats.name("a").threads(-1).build());

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

    public void testStatsParallelismConstructorAndToXContent() throws IOException {
        // Test constructor and toXContent with parallelism set
        ThreadPoolStats.Stats stats = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(8)
            .build();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"parallelism\":8"));

        // Test with parallelism = -1 (should not output the field)
        stats = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(-1)
            .build();
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        json = builder.toString();
        assertFalse(json.contains("parallelism"));
    }

    public void testStatsSerializationParallelismVersion() throws IOException {
        // Serialization for version >= 3.4.0 (parallelism is written and read)
        ThreadPoolStats.Stats statsOut = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(9)
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_4_0);
        statsOut.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_4_0);
        ThreadPoolStats.Stats statsIn = new ThreadPoolStats.Stats(in);
        assertEquals(9, statsIn.getParallelism());

        // Serialization for version < 3.4.0 (parallelism is not written, should be -1)
        out = new BytesStreamOutput();
        out.setVersion(Version.V_3_3_0);
        statsOut.writeTo(out);
        in = out.bytes().streamInput();
        in.setVersion(Version.V_3_3_0);
        statsIn = new ThreadPoolStats.Stats(in);
        assertEquals(-1, statsIn.getParallelism());
    }

    public void testStatsCompareToWithParallelism() {
        ThreadPoolStats.Stats.Builder builder = new ThreadPoolStats.Stats.Builder().name("a")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(8);
        ThreadPoolStats.Stats s1 = builder.build();
        ThreadPoolStats.Stats s2 = builder.build();
        ThreadPoolStats.Stats s3 = builder.threads(2).build();
        ThreadPoolStats.Stats s4 = builder.name("b").build();

        assertEquals(0, s1.compareTo(s2));
        assertTrue(s1.compareTo(s3) < 0);
        assertTrue(s4.compareTo(s1) > 0);
    }

    public void testStatsGetters() {
        ThreadPoolStats.Stats stats = new ThreadPoolStats.Stats.Builder().name("test")
            .threads(1)
            .queue(2)
            .active(3)
            .rejected(4L)
            .largest(5)
            .completed(6L)
            .waitTimeNanos(7L)
            .parallelism(8)
            .build();
        assertEquals("test", stats.getName());
        assertEquals(1, stats.getThreads());
        assertEquals(2, stats.getQueue());
        assertEquals(3, stats.getActive());
        assertEquals(4L, stats.getRejected());
        assertEquals(5, stats.getLargest());
        assertEquals(6L, stats.getCompleted());
        assertEquals(7L, stats.getWaitTimeNanos());
        assertEquals(8, stats.getParallelism());
    }

    public void testThreadPoolStatsToXContent() throws IOException {
        try (BytesStreamOutput os = new BytesStreamOutput()) {

            List<ThreadPoolStats.Stats> stats = new ArrayList<>();
            ThreadPoolStats.Stats.Builder defaultStats = new ThreadPoolStats.Stats.Builder().threads(-1)
                .queue(0)
                .active(0)
                .rejected(0)
                .largest(0)
                .completed(0L)
                .parallelism(-1);
            stats.add(defaultStats.name(ThreadPool.Names.SEARCH).waitTimeNanos(0L).build());
            stats.add(defaultStats.name(ThreadPool.Names.WARMER).waitTimeNanos(-1L).build());
            stats.add(defaultStats.name(ThreadPool.Names.GENERIC).waitTimeNanos(-1L).build());
            stats.add(defaultStats.name(ThreadPool.Names.FORCE_MERGE).waitTimeNanos(-1L).build());
            stats.add(defaultStats.name(ThreadPool.Names.SAME).waitTimeNanos(-1L).build());

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
                        ThreadPool.Names.SAME,
                        ThreadPool.Names.SEARCH,
                        ThreadPool.Names.WARMER
                    )
                );
            }
        }
    }
}
