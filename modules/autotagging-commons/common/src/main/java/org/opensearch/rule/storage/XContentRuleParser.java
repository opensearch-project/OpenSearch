/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;

import java.io.IOException;

/**
 * Rule parser for json XContent representation of Rule
 */
@ExperimentalApi
public class XContentRuleParser implements RuleEntityParser {
    private final FeatureType featureType;
    private static final Logger logger = LogManager.getLogger(XContentRuleParser.class);

    /**
     * Constructor
     * @param featureType
     */
    public XContentRuleParser(FeatureType featureType) {
        this.featureType = featureType;
    }

    @Override
    public Rule parse(String src) {
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, src)
        ) {
            return Rule.Builder.fromXContent(parser, featureType).build();
        } catch (IOException e) {
            logger.info("Issue met when parsing rule : {}", e.getMessage());
            throw new RuntimeException("Cannot parse rule from index.");
        }
    }
}
