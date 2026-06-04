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

package org.opensearch.index;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;

import java.io.IOException;

/**
 * Exception thrown if an index is not found
 *
 * @opensearch.internal
 */
public final class IndexNotFoundException extends ResourceNotFoundException {
    /**
     * Construct with a custom message.
     */
    public IndexNotFoundException(String message, String index) {
        super("no such index [" + index + "] and " + message);
        setIndex(index);
    }

    public IndexNotFoundException(String index) {
        this(index, (Throwable) null);
    }

    public IndexNotFoundException(String index, Throwable cause) {
        super("no such index [" + index + "]", cause);
        setIndex(index);
    }

    public IndexNotFoundException(Index index) {
        this(index, null);
    }

    public IndexNotFoundException(Index index, Throwable cause) {
        super("no such index [" + index.getName() + "]", cause);
        setIndex(index);
    }

    public IndexNotFoundException(StreamInput in) throws IOException {
        super(in);
    }
}
