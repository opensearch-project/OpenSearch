/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * This class contains methods to validate the ingest pipeline.
 */
public class IngestPipelineValidator {

    /**
     * Defines the limit for the number of processors which can run on a given document during ingestion.
     */
    public static final Setting<Integer> MAX_NUMBER_OF_INGEST_PROCESSORS = Setting.intSetting(
        "cluster.ingest.max_number_processors",
        Integer.MAX_VALUE,
        1,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Validates that the number of compound processors in the pipeline does not exceed the configured limit.
     *
     * @param pipeline
     * @param clusterService
     */
    public static void validateIngestPipeline(Pipeline pipeline, ClusterService clusterService) {

        List<Processor> processors = pipeline.getCompoundProcessor().getProcessors();
        int maxNumberOfIngestProcessorsAllowed = clusterService.getClusterSettings().get(MAX_NUMBER_OF_INGEST_PROCESSORS);

        if (processors.size() > maxNumberOfIngestProcessorsAllowed) {
            throw new IllegalStateException(
                "Cannot use more than the maximum processors allowed. Number of processors configured is ["
                    + processors.size()
                    + "] which exceeds the maximum allowed configuration of ["
                    + maxNumberOfIngestProcessorsAllowed
                    + "] processors."
            );
        }
    }
}
