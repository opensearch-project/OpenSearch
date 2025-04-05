/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

import jdk.jfr.Experimental;

/**
 * Utility class for parsing index stored rules into Rule objects.
 * @opensearch.experimental
 */
@Experimental
public class IndexStoredRuleParser {

    /**
     * constructor for IndexStoredRuleParser
     */
    public IndexStoredRuleParser() {}

    private static final Logger logger = LogManager.getLogger(IndexStoredRuleParser.class);

    /**
     * Parses a source string into a Rule object
     * @param source - The raw source string representing the rule to be parsed
     * @param featureType - The feature type to associate with the parsed rule
     */
    public static Rule parseRule(String source, FeatureType featureType) {
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source)
        ) {
            return Rule.Builder.fromXContent(parser, featureType).build();
        } catch (IOException e) {
            logger.info("Issue met when parsing rule : {}", e.getMessage());
            throw new RuntimeException("Cannot parse rule from index.");
        }
    }
}
