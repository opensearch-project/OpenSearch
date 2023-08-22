/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol.controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.throttling.admissioncontrol.AdmissionControlActionsMap;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.throttling.admissioncontrol.settings.IOBasedAdmissionControllerSettings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  Class for IO Based Admission Controller in OpenSearch, which aims to provide IO utilisation admission control.
 *  It provides methods to apply admission control if configured limit has been reached
 */
public class IOBasedAdmissionController implements AdmissionController {
    private static final Logger LOGGER = LogManager.getLogger(IOBasedAdmissionController.class);
    private final String admissionControllerName;
    public IOBasedAdmissionControllerSettings settings;

    private final Set<String> appliedTransportActions;
    private final AtomicLong rejectionCount;

    /**
     *
     * @param admissionControllerName State of the admission controller
     */
    public IOBasedAdmissionController(String admissionControllerName, Settings settings, ClusterSettings clusterSettings) {
        this.admissionControllerName = admissionControllerName;
        this.appliedTransportActions = new HashSet<>();
        this.settings = new IOBasedAdmissionControllerSettings(clusterSettings, settings);
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
     * This function will take of applying admission controller based on IO usage
     * @param action is the transport action
     * @return true if admission controller is successfully acquired on the request else false
     */
    @Override
    public boolean acquire(String action) {
        // TODO Will extend this logic further currently just incrementing rejectionCount
        if (this.isEnabledForTransportLayer()) {
            this.applyForTransportLayer(action);
        }
        return true;
    }

    private void applyForTransportLayer(String actionName) {
        if (this.appliedTransportActions.contains(actionName)) {
            if (isLimitsBreached()) {
                this.addRejectionCount(1);
            }
        }
    }

    /**
     *
     * @return true if the limits breached else false
     */
    boolean isLimitsBreached() {
        // Will Extend this further next PR's
        return true;
    }

    /**
     * @return true if the admission controller cleared the objects that acquired else false
     */
    @Override
    public boolean release() {
        return false;
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

    /**
     *
     * @param transportActions type of the transport actions provided in settings
     */
    public void setAllowedTransportActions(List<String> transportActions) {
        this.appliedTransportActions.clear();
        transportActions.forEach(
            action -> { this.appliedTransportActions.addAll(AdmissionControlActionsMap.getTransportActionType(action)); }
        );
    }

    /**
     *
     * @return the set of the transport actions
     */
    public Set<String> getAllowedTransportActions() {
        return appliedTransportActions;
    }
}
