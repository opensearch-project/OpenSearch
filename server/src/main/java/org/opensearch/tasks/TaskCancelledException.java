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

package org.opensearch.tasks;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

/**
 * A generic exception that can be thrown by a task when it's cancelled by the task manager API
 *
 * @opensearch.internal
 */
public class TaskCancelledException extends OpenSearchException {
    private final RestStatus restStatus;

    public TaskCancelledException(String msg) {
        this(msg, RestStatus.INTERNAL_SERVER_ERROR);
    }

    public TaskCancelledException(String msg, RestStatus restStatus) {
        super(msg);
        this.restStatus = restStatus;
    }

    public TaskCancelledException(StreamInput in) throws IOException {
        super(in);

        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.restStatus = RestStatus.readFrom(in);
        } else {
            this.restStatus = RestStatus.INTERNAL_SERVER_ERROR;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            RestStatus.writeTo(out, restStatus);
        }
    }

    @Override
    public RestStatus status() {
        return restStatus;
    }
}
