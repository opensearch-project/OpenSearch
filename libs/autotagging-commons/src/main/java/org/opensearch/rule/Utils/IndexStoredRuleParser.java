/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

public class IndexStoredRuleParser {
    private static final Logger logger = LogManager.getLogger(IndexStoredRuleParser.class);

    /**
     * Parses a source string into a Rule object
     * @param source - The raw source string representing the rule to be parsed
     */
    public static Rule parseRule(String source, FeatureType featureType) {
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source)
        ) {
            return Rule.Builder.fromXContent(parser, featureType).build();
        } catch (IOException e) {
            logger.info("Issue met when parsing rule {}: {}", source, e.getMessage());
            throw new RuntimeException("Cannot parse rule from index: " + source);
        }
    }
}
