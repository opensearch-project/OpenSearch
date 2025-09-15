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

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Doc value and formats to sort during search
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchSortValuesAndFormats implements Writeable {
    private final Object[] rawSortValues;
    private final Object[] formattedSortValues;
    private final DocValueFormat[] sortValueFormats;

    public SearchSortValuesAndFormats(Object[] rawSortValues, DocValueFormat[] sortValueFormats) {
        assert rawSortValues.length == sortValueFormats.length;
        this.rawSortValues = rawSortValues;
        this.sortValueFormats = sortValueFormats;
        this.formattedSortValues = Arrays.copyOf(rawSortValues, rawSortValues.length);
        for (int i = 0; i < rawSortValues.length; ++i) {
            Object sortValue = rawSortValues[i];
            this.formattedSortValues[i] = switch (sortValue) {
                case BytesRef bytesRef -> sortValueFormats[i].format(bytesRef);
                case Long longValue -> sortValueFormats[i].format(longValue);
                case BigInteger bigInteger -> sortValueFormats[i].format(bigInteger);
                case Double doubleValue -> sortValueFormats[i].format(doubleValue);
                case Float ignored -> sortValue;
                case Integer ignored -> sortValue; // sort by _score or _doc
                case null -> sortValue;
                default -> {
                    assert false : "Sort values must be a BytesRef, Long, Integer, Double or Float, but got "
                        + sortValue.getClass()
                        + ": "
                        + sortValue;
                    yield sortValue;
                }
            };
        }
    }

    public SearchSortValuesAndFormats(StreamInput in) throws IOException {
        this.rawSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        this.formattedSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        this.sortValueFormats = new DocValueFormat[formattedSortValues.length];
        for (int i = 0; i < sortValueFormats.length; ++i) {
            sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeSortValue, rawSortValues);
        out.writeArray(Lucene::writeSortValue, formattedSortValues);
        for (int i = 0; i < sortValueFormats.length; i++) {
            out.writeNamedWriteable(sortValueFormats[i]);
        }
    }

    public Object[] getRawSortValues() {
        return rawSortValues;
    }

    public Object[] getFormattedSortValues() {
        return formattedSortValues;
    }

    public DocValueFormat[] getSortValueFormats() {
        return sortValueFormats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchSortValuesAndFormats that = (SearchSortValuesAndFormats) o;
        return Arrays.equals(rawSortValues, that.rawSortValues)
            && Arrays.equals(formattedSortValues, that.formattedSortValues)
            && Arrays.equals(sortValueFormats, that.sortValueFormats);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(rawSortValues);
        result = 31 * result + Arrays.hashCode(formattedSortValues);
        result = 31 * result + Arrays.hashCode(sortValueFormats);
        return result;
    }
}
