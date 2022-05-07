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

package org.opensearch.index.translog;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception thrown if the translog is corrupted
 *
 * @opensearch.internal
 */
public class TranslogCorruptedException extends OpenSearchException {
    public TranslogCorruptedException(String source, String details) {
        super(corruptedMessage(source, details));
    }

    public TranslogCorruptedException(String source, Throwable cause) {
        this(source, null, cause);
    }

    public TranslogCorruptedException(String source, String details, Throwable cause) {
        super(corruptedMessage(source, details), cause);
    }

    private static String corruptedMessage(String source, String details) {
        String msg = "translog from source [" + source + "] is corrupted";
        if (details != null) {
            msg += ", " + details;
        }
        return msg;
    }

    public TranslogCorruptedException(StreamInput in) throws IOException {
        super(in);
    }
}
