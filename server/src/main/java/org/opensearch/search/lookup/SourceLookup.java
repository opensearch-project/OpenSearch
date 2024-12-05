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

package org.opensearch.search.lookup;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * Orchestrator class for source lookups. Not thread safe.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SourceLookup implements Map {

    private LeafReader reader;
    CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader;

    private int docId = -1;

    private BytesReference sourceAsBytes;
    private Map<String, Object> source;
    private MediaType sourceContentType;

    public Map<String, Object> source() {
        return source;
    }

    public MediaType sourceContentType() {
        return sourceContentType;
    }

    public int docId() {
        return docId;
    }

    // Scripting requires this method to be public. Using source()
    // is not possible because certain checks use source == null as
    // as a determination if source is enabled/disabled, but it should
    // never be a null Map for scripting even when disabled.
    public Map<String, Object> loadSourceIfNeeded() {
        if (source != null) {
            return source;
        }
        if (sourceAsBytes != null) {
            Tuple<XContentType, Map<String, Object>> tuple = sourceAsMapAndType(sourceAsBytes);
            sourceContentType = tuple.v1();
            source = tuple.v2();
            return source;
        }
        try {
            FieldsVisitor sourceFieldVisitor = new FieldsVisitor(true);
            fieldReader.accept(docId, sourceFieldVisitor);
            BytesReference source = sourceFieldVisitor.source();
            if (source == null) {
                this.source = emptyMap();
                this.sourceContentType = null;
            } else {
                Tuple<XContentType, Map<String, Object>> tuple = sourceAsMapAndType(source);
                this.sourceContentType = tuple.v1();
                this.source = tuple.v2();
            }
        } catch (Exception e) {
            throw new OpenSearchParseException("failed to parse / load source", e);
        }
        return this.source;
    }

    private static Tuple<XContentType, Map<String, Object>> sourceAsMapAndType(BytesReference source) throws OpenSearchParseException {
        return XContentHelper.convertToMap(source, false);
    }

    public static Map<String, Object> sourceAsMap(BytesReference source) throws OpenSearchParseException {
        return sourceAsMapAndType(source).v2();
    }

    public void setSegmentAndDocument(LeafReaderContext context, int docId) {
        if (this.reader == context.reader() && this.docId == docId) {
            // if we are called with the same document, don't invalidate source
            return;
        }
        if (this.reader != context.reader()) {
            this.reader = context.reader();
            // only reset reader and fieldReader when reader changes
            try {
                if (context.reader() instanceof SequentialStoredFieldsLeafReader) {
                    // All the docs to fetch are adjacent but Lucene stored fields are optimized
                    // for random access and don't optimize for sequential access - except for merging.
                    // So we do a little hack here and pretend we're going to do merges in order to
                    // get better sequential access.
                    SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) context.reader();
                    fieldReader = lf.getSequentialStoredFieldsReader()::document;
                } else {
                    fieldReader = context.reader().storedFields()::document;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        this.source = null;
        this.sourceAsBytes = null;
        this.docId = docId;
    }

    public void setSource(BytesReference source) {
        this.sourceAsBytes = source;
    }

    public void setSourceContentType(MediaType sourceContentType) {
        this.sourceContentType = sourceContentType;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    /**
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return sourceAsBytes;
    }

    /**
     * Returns the values associated with the path. Those are "low" level values, and it can
     * handle path expression where an array/list is navigated within.
     */
    public List<Object> extractRawValues(String path) {
        return XContentMapValues.extractRawValues(path, loadSourceIfNeeded());
    }

    /**
     * For the provided path, return its value in the source.
     * <p>
     * Note that in contrast with {@link SourceLookup#extractRawValues}, array and object values
     * can be returned.
     *
     * @param path the value's path in the source.
     * @param nullValue a value to return if the path exists, but the value is 'null'. This helps
     *                  in distinguishing between a path that doesn't exist vs. a value of 'null'.
     *
     * @return the value associated with the path in the source or 'null' if the path does not exist.
     */
    public Object extractValue(String path, @Nullable Object nullValue) {
        return XContentMapValues.extractValue(path, loadSourceIfNeeded(), nullValue);
    }

    public Object filter(FetchSourceContext context) {
        return context.getFilter().apply(loadSourceIfNeeded());
    }

    @Override
    public Object get(Object key) {
        return loadSourceIfNeeded().get(key);
    }

    @Override
    public int size() {
        return loadSourceIfNeeded().size();
    }

    @Override
    public boolean isEmpty() {
        return loadSourceIfNeeded().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return loadSourceIfNeeded().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return loadSourceIfNeeded().containsValue(value);
    }

    @Override
    public Set keySet() {
        return loadSourceIfNeeded().keySet();
    }

    @Override
    public Collection values() {
        return loadSourceIfNeeded().values();
    }

    @Override
    public Set entrySet() {
        return loadSourceIfNeeded().entrySet();
    }

    @Override
    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
