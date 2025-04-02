/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.autotagging.FeatureType;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.rule.service.RuleProcessingService;

public class WlmRuleProcessingService implements RuleProcessingService {
    @Override
    public FeatureType retrieveFeatureTypeInstance() {
        return QueryGroupFeatureType.INSTANCE;
    }
}
