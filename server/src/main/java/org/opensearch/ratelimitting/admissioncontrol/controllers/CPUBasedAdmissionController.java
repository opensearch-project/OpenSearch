/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.settings.CPUBasedAdmissionControllerSettings;

import java.util.concurrent.atomic.AtomicLong;

/**
 *  Class for CPU Based Admission Controller in OpenSearch, which aims to provide CPU utilisation admission control.
 *  It provides methods to apply admission control if configured limit has been reached
 */
public class CPUBasedAdmissionController extends AdmissionController {
    private static final Logger LOGGER = LogManager.getLogger(CPUBasedAdmissionController.class);
    public CPUBasedAdmissionControllerSettings settings;

    /**
     *
     * @param admissionControllerName State of the admission controller
     */
    public CPUBasedAdmissionController(String admissionControllerName, Settings settings, ClusterSettings clusterSettings) {
        super(new AtomicLong(0), admissionControllerName);
        this.settings = new CPUBasedAdmissionControllerSettings(clusterSettings, settings);
    }

    /**
     * This function will take of applying admission controller based on CPU usage
     * @param action is the transport action
     */
    @Override
    public void apply(String action) {
        // TODO Will extend this logic further currently just incrementing rejectionCount
        if (this.isEnabledForTransportLayer(this.settings.getTransportLayerAdmissionControllerMode())) {
            this.applyForTransportLayer(action);
        }
    }

    private void applyForTransportLayer(String actionName) {
        // currently incrementing counts to evaluate the controller triggering as expected and using in testing so limiting to 10
        // TODO will update rejection logic further in next PR's
        if (this.getRejectionCount() < 10) {
            this.addRejectionCount(1);
        }
    }
}
