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

package org.opensearch.transport;

/**
 * Status for transport activity
 *
 * @opensearch.internal
 */
public final class TransportStatus {

    private static final byte STATUS_REQRES = 1 << 0;
    private static final byte STATUS_ERROR = 1 << 1;
    private static final byte STATUS_COMPRESS = 1 << 2;
    private static final byte STATUS_HANDSHAKE = 1 << 3;

    public static boolean isRequest(byte value) {
        return (value & STATUS_REQRES) == 0;
    }

    public static byte setRequest(byte value) {
        value &= ~STATUS_REQRES;
        return value;
    }

    public static byte setResponse(byte value) {
        value |= STATUS_REQRES;
        return value;
    }

    public static boolean isError(byte value) {
        return (value & STATUS_ERROR) != 0;
    }

    public static byte setError(byte value) {
        value |= STATUS_ERROR;
        return value;
    }

    public static boolean isCompress(byte value) {
        return (value & STATUS_COMPRESS) != 0;
    }

    public static byte setCompress(byte value) {
        value |= STATUS_COMPRESS;
        return value;
    }

    public static boolean isHandshake(byte value) {
        return (value & STATUS_HANDSHAKE) != 0;
    }

    public static byte setHandshake(byte value) {
        value |= STATUS_HANDSHAKE;
        return value;
    }
}
