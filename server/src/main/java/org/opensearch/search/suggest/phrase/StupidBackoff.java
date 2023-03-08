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

package org.opensearch.search.suggest.phrase;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.search.suggest.phrase.WordScorer.WordScorerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * A "stupid-backoff" smoothing model similar to <a
 * href="http://en.wikipedia.org/wiki/Katz's_back-off_model"> Katz's
 * Backoff</a>. This model is used as the default if no model is configured.
 * <p>
 * See <a
 * href="http://en.wikipedia.org/wiki/N-gram#Smoothing_techniques">N-Gram
 * Smoothing</a> for details.
 * </p>
 *
 * @opensearch.internal
 */
public final class StupidBackoff extends SmoothingModel {
    /**
     * Default discount parameter for {@link StupidBackoff} smoothing
     */
    public static final double DEFAULT_BACKOFF_DISCOUNT = 0.4;
    public static final String NAME = "stupid_backoff";
    private static final ParseField DISCOUNT_FIELD = new ParseField("discount");
    static final ParseField PARSE_FIELD = new ParseField(NAME);

    private double discount = DEFAULT_BACKOFF_DISCOUNT;

    /**
     * Creates a Stupid-Backoff smoothing model.
     *
     * @param discount
     *            the discount given to lower order ngrams if the higher order ngram doesn't exits
     */
    public StupidBackoff(double discount) {
        this.discount = discount;
    }

    /**
     * Read from a stream.
     */
    public StupidBackoff(StreamInput in) throws IOException {
        discount = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(discount);
    }

    /**
     * @return the discount parameter of the model
     */
    public double getDiscount() {
        return this.discount;
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(DISCOUNT_FIELD.getPreferredName(), discount);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(SmoothingModel other) {
        StupidBackoff otherModel = (StupidBackoff) other;
        return Objects.equals(discount, otherModel.discount);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(discount);
    }

    public static SmoothingModel fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String fieldName = null;
        double discount = DEFAULT_BACKOFF_DISCOUNT;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            }
            if (token.isValue() && DISCOUNT_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                discount = parser.doubleValue();
            }
        }
        return new StupidBackoff(discount);
    }

    @Override
    public WordScorerFactory buildWordScorerFactory() {
        return (IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator) -> new StupidBackoffScorer(
            reader,
            terms,
            field,
            realWordLikelihood,
            separator,
            discount
        );
    }
}
