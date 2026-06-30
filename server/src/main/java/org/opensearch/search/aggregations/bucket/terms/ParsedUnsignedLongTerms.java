/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;

/**
 * A long term agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedUnsignedLongTerms extends ParsedTerms {

    @Override
    public String getType() {
        return UnsignedLongTerms.NAME;
    }

    private static final ObjectParser<ParsedUnsignedLongTerms, Void> PARSER = new ObjectParser<>(
        ParsedUnsignedLongTerms.class.getSimpleName(),
        true,
        ParsedUnsignedLongTerms::new
    );
    static {
        declareParsedTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedUnsignedLongTerms fromXContent(XContentParser parser, String name) throws IOException {
        ParsedUnsignedLongTerms aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    /**
     * Parsed bucket for long term values
     *
     * @opensearch.internal
     */
    public static class ParsedBucket extends ParsedTerms.ParsedBucket {

        private BigInteger key;

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            if (key != null) {
                return key.toString();
            }
            return null;
        }

        public Number getKeyAsNumber() {
            return key;
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(CommonFields.KEY.getPreferredName(), key);
            if (super.getKeyAsString() != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            return builder;
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseTermsBucketXContent(parser, ParsedBucket::new, (p, bucket) -> bucket.key = p.bigIntegerValue());
        }
    }
}
