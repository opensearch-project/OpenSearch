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
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlActionsMap;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CPUBasedAdmissionControllerSettings;

import java.util.concurrent.atomic.AtomicLong;

/**
 *  Class for CPU Based Admission Controller in OpenSearch, which aims to provide CPU utilisation admission control.
 *  It provides methods to apply admission control if configured limit has been reached
 */
public class CPUBasedAdmissionController implements AdmissionController {
    private static final Logger LOGGER = LogManager.getLogger(CPUBasedAdmissionController.class);
    private final String admissionControllerName;
    public CPUBasedAdmissionControllerSettings settings;
    private final AtomicLong rejectionCount;

    /**
     *
     * @param admissionControllerName State of the admission controller
     */
    public CPUBasedAdmissionController(String admissionControllerName, Settings settings, ClusterSettings clusterSettings) {
        this.admissionControllerName = admissionControllerName;
        this.settings = new CPUBasedAdmissionControllerSettings(clusterSettings, settings);
        this.rejectionCount = new AtomicLong(0);
    }

    /**
     *
     * @return true if admissionController is enabled for the transport layer else false
     */
    @Override
    public boolean isEnabledForTransportLayer() {
        return this.settings.getTransportLayerAdmissionControllerMode() != AdmissionControlMode.DISABLED;
    }

    /**
     * This function will take of applying admission controller based on CPU usage
     * @param action is the transport action
     * @return true if admission controller is successfully acquired on the request else false
     */
    @Override
    public boolean accept(String action) {
        // TODO Will extend this logic further currently just incrementing rejectionCount
        if (this.isEnabledForTransportLayer()) {
            this.applyForTransportLayer(action);
        }
        return true;
    }

    private void applyForTransportLayer(String actionName) {
        if (AdmissionControlActionsMap.containsActionType(actionName)) {
            String actionType = AdmissionControlActionsMap.getTransportActionType(actionName);
            if (isLimitsBreached()) {
                this.addRejectionCount(1);
            }
        }
    }

    /**
     *
     * @return true if the limits breached else false
     */
    private boolean isLimitsBreached() {
        // Will Extend this further next PR's currently making it true for testing complete flow
        return true;
    }

    /**
     * @return name of the admission Controller
     */
    @Override
    public String getName() {
        return this.admissionControllerName;
    }

    /**
     * Adds the rejection count for the controller.
     *
     * @param count the value that needs to be added to total rejection count
     */
    @Override
    public void addRejectionCount(long count) {
        this.rejectionCount.incrementAndGet();
    }

    /**
     * @return current value of the rejection count metric tracked by the admission-controller.
     */
    @Override
    public long getRejectionCount() {
        return this.rejectionCount.get();
    }
}
