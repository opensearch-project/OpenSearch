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

package org.opensearch.common.io;

import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.threadpool.ThreadPool;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Pool of disk io buffers
 *
 * @opensearch.internal
 */
public class DiskIoBufferPool {

    public static final int BUFFER_SIZE = StrictMath.toIntExact(
        ByteSizeValue.parseBytesSizeValue(
            System.getProperty("opensearch.disk_io.direct.buffer.size", "64KB"),
            "opensearch.disk_io.direct.buffer.size"
        ).getBytes()
    );
    public static final int HEAP_BUFFER_SIZE = 8 * 1024;

    private static final ThreadLocal<ByteBuffer> ioBufferPool = ThreadLocal.withInitial(() -> {
        if (isWriteOrFlushThread()) {
            return ByteBuffer.allocateDirect(BUFFER_SIZE);
        } else {
            return ByteBuffer.allocate(HEAP_BUFFER_SIZE);
        }
    });

    public static ByteBuffer getIoBuffer() {
        ByteBuffer ioBuffer = ioBufferPool.get();
        ioBuffer.clear();
        return ioBuffer;
    }

    private static boolean isWriteOrFlushThread() {
        String threadName = Thread.currentThread().getName();
        for (String s : Arrays.asList(
            "[" + ThreadPool.Names.WRITE + "]",
            "[" + ThreadPool.Names.FLUSH + "]",
            "[" + ThreadPool.Names.SYSTEM_WRITE + "]"
        )) {
            if (threadName.contains(s)) {
                return true;
            }
        }
        return false;
    }
}
