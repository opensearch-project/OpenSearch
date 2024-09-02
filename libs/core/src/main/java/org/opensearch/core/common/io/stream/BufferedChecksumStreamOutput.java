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

package org.opensearch.core.common.io.stream;

import org.apache.lucene.store.BufferedChecksum;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Similar to Lucene's BufferedChecksumIndexOutput, however this wraps a
 * {@link StreamOutput} so anything written will update the checksum
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class BufferedChecksumStreamOutput extends StreamOutput {
    private final StreamOutput out;
    private final Checksum digest;

    public BufferedChecksumStreamOutput(StreamOutput out) {
        this.out = out;
        this.digest = new BufferedChecksum(new CRC32());
    }

    public long getChecksum() {
        return this.digest.getValue();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.writeByte(b);
        digest.update(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.writeBytes(b, offset, length);
        digest.update(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void reset() throws IOException {
        out.reset();
        digest.reset();
    }

    public void resetDigest() {
        digest.reset();
    }

    @Override
    public void writeMap(@Nullable Map<String, Object> map) throws IOException {
        Map<String, Object> newMap = new TreeMap<>(map);
        writeGenericValue(newMap);
    }

    @Override
    public <K, V> void writeMap(Map<K, V> map, final Writeable.Writer<K> keyWriter, final Writeable.Writer<V> valueWriter)
        throws IOException {
        writeVInt(map.size());
        map.keySet().stream().sorted().forEachOrdered(key -> {
            try {
                keyWriter.write(this, key);
                valueWriter.write(this, map.get(key));
            } catch (IOException e) {
                throw new RuntimeException("Failed to write map values.", e);
            }
        });
    }

    public <K, V> void writeMapValues(Map<K, V> map, final Writeable.Writer<V> valueWriter) throws IOException {
        writeVInt(map.size());
        map.keySet().stream().sorted().forEachOrdered(key -> {
            try {
                valueWriter.write(this, map.get(key));
            } catch (IOException e) {
                throw new RuntimeException("Failed to write map values.", e);
            }
        });
    }

    @Override
    public void writeStringArray(String[] array) throws IOException {
        Arrays.sort(array);
        super.writeStringArray(array);
    }

    @Override
    public void writeVLongArray(long[] values) throws IOException {
        Arrays.sort(values);
        super.writeVLongArray(values);
    }

    @Override
    public void writeCollection(final Collection<? extends Writeable> collection) throws IOException {
        List<? extends Writeable> sortedList = collection.stream().sorted().collect(Collectors.toList());
        super.writeCollection(sortedList, (o, v) -> v.writeTo(o));
    }

    @Override
    public void writeStringCollection(final Collection<String> collection) throws IOException {
        List<String> listCollection = new ArrayList<>(collection);
        Collections.sort(listCollection);
        writeCollection(listCollection, StreamOutput::writeString);
    }

    @Override
    public void writeOptionalStringCollection(final Collection<String> collection) throws IOException {
        if (collection != null) {
            List<String> listCollection = new ArrayList<>(collection);
            Collections.sort(listCollection);
            writeBoolean(true);
            writeCollection(listCollection, StreamOutput::writeString);
        } else {
            writeBoolean(false);
        }
    }

}
