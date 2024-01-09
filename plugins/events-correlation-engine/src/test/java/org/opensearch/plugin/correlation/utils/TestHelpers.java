/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.utils;

import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.correlation.events.model.Correlation;
import org.opensearch.plugin.correlation.events.model.EventWithScore;

import java.io.IOException;
import java.util.List;

public class TestHelpers {

    public static String toJsonString(EventWithScore eventWithScore) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = eventWithScore.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return BytesReference.bytes(builder).utf8ToString();
    }

    public static String toJsonString(Correlation correlation) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = correlation.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return BytesReference.bytes(builder).utf8ToString();
    }

    public static XContentParser parse(String xc) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc);
        parser.nextToken();
        return parser;
    }

    public static NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(List.of());
    }
}
