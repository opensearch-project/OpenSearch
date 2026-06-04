/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.rule.autotagging.FeatureValueValidator;

/**
 * Validator for the workload_group feature type
 * @opensearch.experimental
 */
public class WorkloadGroupFeatureValueValidator implements FeatureValueValidator {
    private final ClusterService clusterService;
    private final Logger logger = LogManager.getLogger(WorkloadGroupFeatureValueValidator.class);

    /**
     * constructor for WorkloadGroupFeatureValueValidator
     * @param clusterService
     */
    public WorkloadGroupFeatureValueValidator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public void validate(String featureValue) {
        if (!clusterService.state().metadata().workloadGroups().containsKey(featureValue)) {
            logger.error("{} is not a valid workload group id.", featureValue);
            throw new ResourceNotFoundException(featureValue + " is not a valid workload group id.");
        }
    }
}
