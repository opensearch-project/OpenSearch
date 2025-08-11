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

package org.opensearch.common.lucene.store;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} that writes into underlying IndexOutput
 *
 * @opensearch.internal
 */
public class IndexOutputOutputStream extends OutputStream {

    private final IndexOutput out;

    public IndexOutputOutputStream(IndexOutput out) {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        out.writeByte((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.writeBytes(b, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.writeBytes(b, off, len);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
