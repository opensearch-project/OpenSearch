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
import java.util.List;

/**
 * A multi terms result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedMultiTerms extends ParsedTerms {
    @Override
    public String getType() {
        return MultiTermsAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedMultiTerms, Void> PARSER = new ObjectParser<>(
        ParsedMultiTerms.class.getSimpleName(),
        true,
        ParsedMultiTerms::new
    );
    static {
        declareParsedTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedMultiTerms fromXContent(XContentParser parser, String name) throws IOException {
        ParsedMultiTerms aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    /**
     * Parsed bucket for multi terms
     *
     * @opensearch.internal
     */
    public static class ParsedBucket extends ParsedTerms.ParsedBucket {

        private List<Object> key;

        @Override
        public List<Object> getKey() {
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
            throw new UnsupportedOperationException("not implemented");
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
            return parseTermsBucketXContent(parser, ParsedBucket::new, (p, bucket) -> { bucket.key = p.list(); });
        }
    }
}
