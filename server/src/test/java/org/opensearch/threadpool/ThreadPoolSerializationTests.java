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
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ThreadPoolSerializationTests extends OpenSearchTestCase {
    private final BytesStreamOutput output = new BytesStreamOutput();
    private ThreadPool.ThreadPoolType threadPoolType;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPoolType = randomFrom(ThreadPool.ThreadPoolType.values());
    }

    public void testThatQueueSizeSerializationWorks() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info(
            "foo",
            threadPoolType,
            1,
            10,
            TimeValue.timeValueMillis(3000),
            SizeValue.parseSizeValue("10k")
        );
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        assertThat(newInfo.getQueueSize().singles(), is(10000L));
    }

    public void testThatNegativeQueueSizesCanBeSerialized() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10, TimeValue.timeValueMillis(3000), null);
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        assertThat(newInfo.getQueueSize(), is(nullValue()));
    }

    public void testThatToXContentWritesOutUnboundedCorrectly() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10, TimeValue.timeValueMillis(3000), null);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map, hasKey("foo"));
        map = (Map<String, Object>) map.get("foo");
        assertThat(map, hasKey("queue_size"));
        assertThat(map.get("queue_size").toString(), is("-1"));
    }

    public void testThatNegativeSettingAllowsToStart() throws InterruptedException {
        Settings settings = Settings.builder().put("node.name", "write").put("thread_pool.write.queue_size", "-1").build();
        ThreadPool threadPool = new ThreadPool(settings);
        assertThat(threadPool.info("write").getQueueSize(), is(nullValue()));
        terminate(threadPool);
    }

    public void testThatToXContentWritesInteger() throws Exception {
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
        assertThat(map, hasKey("queue_size"));
        assertThat(map.get("queue_size").toString(), is("1000"));
    }

    public void testThatThreadPoolTypeIsSerializedCorrectly() throws IOException {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType);
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        /* The SerDe patch converts RESIZABLE threadpool type value to FIXED. Implementing
         * the same conversion in test to maintain parity.
         */
        assertThat(newInfo.getThreadPoolType(), is(threadPoolType));
    }
}
