/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.bucket.ParsedSingleBucketAggregation;

import java.io.IOException;

/**
 * A random sampler result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedRandomSampler extends ParsedSingleBucketAggregation implements RandomSampler {

    @Override
    public String getType() {
        return InternalRandomSampler.PARSER_NAME;
    }

    public static ParsedRandomSampler fromXContent(XContentParser parser, final String name) throws IOException {
        return parseXContent(parser, new ParsedRandomSampler(), name);
    }
}
