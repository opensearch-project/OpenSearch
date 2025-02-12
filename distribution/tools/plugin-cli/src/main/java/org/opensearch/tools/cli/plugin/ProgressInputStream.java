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

package org.opensearch.tools.cli.plugin;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that allows to add a listener to monitor progress
 * The listener is triggered whenever a full percent is increased
 * The listener is never triggered twice on the same percentage
 * The listener will always return 99 percent, if the expectedTotalSize is exceeded, until it is finished
 * <p>
 * Only used by the InstallPluginCommand, thus package private here
 */
abstract class ProgressInputStream extends FilterInputStream {

    private final int expectedTotalSize;
    private int currentPercent;
    private int count = 0;

    ProgressInputStream(InputStream is, int expectedTotalSize) {
        super(is);
        this.expectedTotalSize = expectedTotalSize;
        this.currentPercent = 0;
    }

    @Override
    public int read() throws IOException {
        int read = in.read();
        checkProgress(read == -1 ? -1 : 1);
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int byteCount = super.read(b, off, len);
        checkProgress(byteCount);
        return byteCount;
    }

    @Override
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    void checkProgress(int byteCount) {
        // are we done?
        if (byteCount == -1) {
            currentPercent = 100;
            onProgress(currentPercent);
        } else {
            count += byteCount;
            // rounding up to 100% would mean we say we are done, before we are...
            // this also catches issues, when expectedTotalSize was guessed wrong
            int percent = Math.min(99, (int) Math.floor(100.0 * count / expectedTotalSize));
            if (percent > currentPercent) {
                currentPercent = percent;
                onProgress(percent);
            }
        }
    }

    public void onProgress(int percent) {}
}
