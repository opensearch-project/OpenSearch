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

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.ReportingService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Information about a threadpool
 *
 * @opensearch.internal
 */
public class ThreadPoolInfo implements ReportingService.Info, Iterable<ThreadPool.Info> {

    private final List<ThreadPool.Info> infos;

    public ThreadPoolInfo(List<ThreadPool.Info> infos) {
        this.infos = Collections.unmodifiableList(infos);
    }

    public ThreadPoolInfo(StreamInput in) throws IOException {
        this.infos = Collections.unmodifiableList(in.readList(ThreadPool.Info::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(infos);
    }

    @Override
    public Iterator<ThreadPool.Info> iterator() {
        return infos.iterator();
    }

    static final class Fields {
        static final String THREAD_POOL = "thread_pool";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.THREAD_POOL);
        for (ThreadPool.Info info : infos) {
            info.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
