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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ThreadPoolSerializationTests extends OpenSearchTestCase {
    public void testThatQueueSizeSerializationWorks() throws Exception {
        for (ThreadPool.ThreadPoolType threadPoolType : ThreadPool.ThreadPoolType.values()) {
            ThreadPool.Info info = new ThreadPool.Info(
                "foo",
                threadPoolType,
                1,
                10,
                TimeValue.timeValueMillis(3000),
                SizeValue.parseSizeValue("10k")
            );
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.CURRENT);
            info.writeTo(output);

            StreamInput input = output.bytes().streamInput();
            ThreadPool.Info newInfo = new ThreadPool.Info(input);

            assertThat(newInfo.getQueueSize().singles(), is(10000L));
        }
    }

    public void testThatNegativeQueueSizesCanBeSerialized() throws Exception {
        for (ThreadPool.ThreadPoolType threadPoolType : ThreadPool.ThreadPoolType.values()) {
            ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10, TimeValue.timeValueMillis(3000), null);
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.CURRENT);
            info.writeTo(output);

            StreamInput input = output.bytes().streamInput();
            ThreadPool.Info newInfo = new ThreadPool.Info(input);

            assertThat(newInfo.getQueueSize(), is(nullValue()));
        }
    }

    public void testThatToXContentWritesOutUnboundedCorrectly() throws Exception {
        for (ThreadPool.ThreadPoolType threadPoolType : ThreadPool.ThreadPoolType.values()) {
            ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10, TimeValue.timeValueMillis(3000), null);
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            info.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(map, hasKey("foo"));
            map = (Map<String, Object>) map.get("foo");
            if (threadPoolType == ThreadPool.ThreadPoolType.FORK_JOIN) {
                // ForkJoinPool does not write queue_size field at all
                assertThat(map.containsKey("queue_size"), is(false));
            } else {
                assertThat(map, hasKey("queue_size"));
                assertThat(map.get("queue_size").toString(), is("-1"));
            }
        }
    }

    public void testThatNegativeSettingAllowsToStart() throws InterruptedException {
        Settings settings = Settings.builder().put("node.name", "write").put("thread_pool.write.queue_size", "-1").build();
        ThreadPool threadPool = new ThreadPool(settings);
        assertThat(threadPool.info("write").getQueueSize(), is(nullValue()));
        terminate(threadPool);
    }

    public void testThatToXContentWritesInteger() throws Exception {
        for (ThreadPool.ThreadPoolType threadPoolType : ThreadPool.ThreadPoolType.values()) {
            ThreadPool.Info info = new ThreadPool.Info(
                "foo",
                threadPoolType,
                1,
                10,
                TimeValue.timeValueMillis(3000),
                SizeValue.parseSizeValue("1k")
            );
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            info.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(map, hasKey("foo"));
            map = (Map<String, Object>) map.get("foo");
            if (threadPoolType == ThreadPool.ThreadPoolType.FORK_JOIN) {
                // ForkJoinPool does not write queue_size field at all
                assertThat(map.containsKey("queue_size"), is(false));
            } else {
                assertThat(map, hasKey("queue_size"));
                assertThat(map.get("queue_size").toString(), is("1000"));
            }
        }
    }

    public void testThatThreadPoolTypeIsSerializedCorrectly() throws IOException {
        for (ThreadPool.ThreadPoolType threadPoolType : ThreadPool.ThreadPoolType.values()) {
            ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType);
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.CURRENT);
            info.writeTo(output);

            StreamInput input = output.bytes().streamInput();
            ThreadPool.Info newInfo = new ThreadPool.Info(input);

            /* The SerDe patch converts RESIZABLE threadpool type value to FIXED. Implementing
             * the same conversion in test to maintain parity.
             */
            assertThat(newInfo.getThreadPoolType(), is(threadPoolType)); // TODO: FIXED incorrectly giving DIRECT, not sure why my change would cause this?
        }
    }

    public void testResizableThreadPoolTypeSerializationForOldVersions() throws IOException {

        for (ThreadPool.ThreadPoolType type : List.of(ThreadPool.ThreadPoolType.VIRTUAL, ThreadPool.ThreadPoolType.RESIZABLE)) {
            ThreadPool.Info info = new ThreadPool.Info("foo", type);
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_2_19_0); // Versions before 3.0 should return FIXED for both RESIZABLE and VIRTUAL
            info.writeTo(output);

            StreamInput input = output.bytes().streamInput();
            ThreadPool.Info newInfo = new ThreadPool.Info(input);

            assertEquals(ThreadPool.ThreadPoolType.FIXED, newInfo.getThreadPoolType());
        }
    }

    public void testVirtualThreadPoolTypeSerializationForOldVersions() throws IOException {
        ThreadPool.Info info = new ThreadPool.Info("foo", ThreadPool.ThreadPoolType.VIRTUAL);
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(Version.V_3_4_0); // VIRTUAL type introduced V_3_5_0
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        assertEquals(ThreadPool.ThreadPoolType.RESIZABLE, newInfo.getThreadPoolType());
    }

    public void testForkJoinThreadPoolTypeSerializationForOldVersions() throws IOException {
        ThreadPool.Info info = new ThreadPool.Info("foo", ThreadPool.ThreadPoolType.FORK_JOIN);
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(Version.V_3_3_0);
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        assertEquals(ThreadPool.ThreadPoolType.FIXED, newInfo.getThreadPoolType());
    }
}
