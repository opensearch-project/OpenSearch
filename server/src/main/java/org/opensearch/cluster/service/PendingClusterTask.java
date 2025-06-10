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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.service;

import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;

import java.io.IOException;

/**
 * Represents a task that is pending in the cluster
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PendingClusterTask implements Writeable {

    private long insertOrder;
    private Priority priority;
    private Text source;
    private long timeInQueue;
    private boolean executing;

    public PendingClusterTask(StreamInput in) throws IOException {
        insertOrder = in.readVLong();
        priority = Priority.readFrom(in);
        source = in.readText();
        timeInQueue = in.readLong();
        executing = in.readBoolean();
    }

    public PendingClusterTask(long insertOrder, Priority priority, Text source, long timeInQueue, boolean executing) {
        assert timeInQueue >= 0 : "got a negative timeInQueue [" + timeInQueue + "]";
        assert insertOrder >= 0 : "got a negative insertOrder [" + insertOrder + "]";
        this.insertOrder = insertOrder;
        this.priority = priority;
        this.source = source;
        this.timeInQueue = timeInQueue;
        this.executing = executing;
    }

    public long getInsertOrder() {
        return insertOrder;
    }

    public Priority getPriority() {
        return priority;
    }

    public Text getSource() {
        return source;
    }

    public long getTimeInQueueInMillis() {
        return timeInQueue;
    }

    public TimeValue getTimeInQueue() {
        return new TimeValue(getTimeInQueueInMillis());
    }

    public boolean isExecuting() {
        return executing;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(insertOrder);
        Priority.writeTo(priority, out);
        out.writeText(source);
        out.writeLong(timeInQueue);
        out.writeBoolean(executing);
    }
}
