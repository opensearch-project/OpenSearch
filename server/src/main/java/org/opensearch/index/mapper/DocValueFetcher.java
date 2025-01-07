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

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.opensearch.index.mapper.FlatObjectFieldMapper.DOC_VALUE_NO_MATCH;

/**
 * Value fetcher that loads from doc values.
 *
 * @opensearch.internal
 */
public final class DocValueFetcher implements ValueFetcher {
    private final DocValueFormat format;
    private final IndexFieldData<?> ifd;
    private Leaf leaf;

    public DocValueFetcher(DocValueFormat format, IndexFieldData<?> ifd) {
        this.format = format;
        this.ifd = ifd;
    }

    public void setNextReader(LeafReaderContext context) {
        leaf = ifd.load(context).getLeafValueFetcher(format);
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup) throws IOException {
        if (false == leaf.advanceExact(lookup.docId())) {
            return emptyList();
        }
        List<Object> result = new ArrayList<Object>(leaf.docValueCount());
        for (int i = 0, count = leaf.docValueCount(); i < count; ++i) {
            Object value = leaf.nextValue();
            if (value != DOC_VALUE_NO_MATCH) {
                result.add(value);
            }
        }
        return result;
    }

    /**
     * Leaf interface
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public interface Leaf {
        /**
         * Advance the doc values reader to the provided doc.
         * @return false if there are no values for this document, true otherwise
         */
        boolean advanceExact(int docId) throws IOException;

        /**
         * A count of the number of values at this document.
         */
        int docValueCount() throws IOException;

        /**
         * Load and format the next value.
         */
        Object nextValue() throws IOException;
    }
}
