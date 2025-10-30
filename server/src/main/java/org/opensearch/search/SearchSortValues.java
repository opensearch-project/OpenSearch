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

package org.opensearch.search;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.search.SearchHit.Fields;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Values to sort during search
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchSortValues implements ToXContentFragment, Writeable {

    private static final Object[] EMPTY_ARRAY = new Object[0];
    static final SearchSortValues EMPTY = new SearchSortValues(EMPTY_ARRAY);

    private final Object[] formattedSortValues;
    private final Object[] rawSortValues;

    SearchSortValues(Object[] sortValues) {
        this.formattedSortValues = Objects.requireNonNull(sortValues, "sort values must not be empty");
        this.rawSortValues = EMPTY_ARRAY;
    }

    public SearchSortValues(Object[] rawSortValues, DocValueFormat[] sortValueFormats) {
        Objects.requireNonNull(rawSortValues);
        Objects.requireNonNull(sortValueFormats);
        if (rawSortValues.length != sortValueFormats.length) {
            throw new IllegalArgumentException("formattedSortValues and sortValueFormats must hold the same number of items");
        }
        this.rawSortValues = rawSortValues;
        this.formattedSortValues = Arrays.copyOf(rawSortValues, rawSortValues.length);
        for (int i = 0; i < rawSortValues.length; ++i) {
            Object sortValue = rawSortValues[i];
            if (sortValue instanceof BytesRef bytesRef) {
                this.formattedSortValues[i] = sortValueFormats[i].format(bytesRef);
            } else if ((sortValue instanceof Long longValue) && (sortValueFormats[i] == DocValueFormat.UNSIGNED_LONG_SHIFTED)) {
                this.formattedSortValues[i] = sortValueFormats[i].format(longValue);
            } else if ((sortValue instanceof Long longValue) && (sortValueFormats[i] == DocValueFormat.UNSIGNED_LONG)) {
                this.formattedSortValues[i] = sortValueFormats[i].format(longValue);
            } else {
                this.formattedSortValues[i] = sortValue;
            }
        }
    }

    SearchSortValues(StreamInput in) throws IOException {
        this.formattedSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        this.rawSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeSortValue, this.formattedSortValues);
        out.writeArray(Lucene::writeSortValue, this.rawSortValues);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (formattedSortValues.length > 0) {
            builder.startArray(Fields.SORT);
            for (Object sortValue : formattedSortValues) {
                builder.value(sortValue);
            }
            builder.endArray();
        }
        return builder;
    }

    public static SearchSortValues fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        return new SearchSortValues(parser.list().toArray());
    }

    /**
     * Returns the formatted version of the values that sorting was performed against
     */
    public Object[] getFormattedSortValues() {
        return formattedSortValues;
    }

    /**
     * Returns the raw version of the values that sorting was performed against
     */
    public Object[] getRawSortValues() {
        return rawSortValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchSortValues that = (SearchSortValues) o;
        return Arrays.equals(formattedSortValues, that.formattedSortValues) && Arrays.equals(rawSortValues, that.rawSortValues);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(formattedSortValues);
        result = 31 * result + Arrays.hashCode(rawSortValues);
        return result;
    }
}
