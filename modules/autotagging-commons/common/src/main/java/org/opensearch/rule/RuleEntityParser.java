/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rule.autotagging.Rule;

/**
 * Interface to parse various string representation of Rule entity
 * clients can use/implement as per their choice of storage for the Rule
 */
@ExperimentalApi
public interface RuleEntityParser {
    /**
     * Parses the src string into {@link Rule} object
     * @param src String representation of Rule, it could be a XContentObject or something else based on
     *           where and how it is stored
     * @return Rule
     */
    Rule parse(String src);
}
