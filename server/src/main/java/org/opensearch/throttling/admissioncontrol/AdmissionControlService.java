/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.throttling.admissioncontrol.controllers.AdmissionController;
import org.opensearch.throttling.admissioncontrol.controllers.IOBasedAdmissionController;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.opensearch.throttling.admissioncontrol.AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER;

/**
 * Admission control Service that bootstraps and manages all the Admission Controllers in OpenSearch.
 */
public class AdmissionControlService {
    private final ThreadPool threadPool;
    public final AdmissionControlSettings admissionControlSettings;
    private final ConcurrentMap<String, AdmissionController> ADMISSION_CONTROLLERS;
    private static AdmissionControlService admissionControlService = null;
    private static final Logger logger = LogManager.getLogger(AdmissionControlService.class);
    private final ClusterSettings clusterSettings;
    private final Settings settings;

    /**
     *
     * @param settings Immutable settings instance
     * @param clusterSettings ClusterSettings Instance
     * @param threadPool ThreadPool Instance
     */
    public AdmissionControlService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.admissionControlSettings = new AdmissionControlSettings(clusterSettings, settings);
        this.ADMISSION_CONTROLLERS = new ConcurrentHashMap<>();
        this.clusterSettings = clusterSettings;
        this.settings = settings;
        this.initialise();
    }

    /**
     * Initialise and Register all the admissionControllers
     */
    public void initialise() {
        // Initialise different type of admission controllers
        registerAdmissionController(IO_BASED_ADMISSION_CONTROLLER);
    }

    /**
     * Handler to trigger registered admissionController
     */
    public boolean applyTransportAdmissionControl(String action) {
        if (this.isTransportLayerAdmissionControlBlocked()) {
            return false;
        }
        this.ADMISSION_CONTROLLERS.forEach((name, admissionController) -> { admissionController.acquire(action); });
        return true;
    }

    /**
     *
     * @return true if admissionControl is blocked for transport layer else false
     */
    public boolean isTransportLayerAdmissionControlBlocked() {
        return this.isTransportLayerAdmissionControlDisabled();
    }

    /**
     *
     * @return true if admissionControl is disabled else false
     */
    public boolean isTransportLayerAdmissionControlDisabled() {
        return this.admissionControlSettings.getAdmissionControlTransportLayerMode() == AdmissionControlMode.DISABLED;
    }

    /**
     *
     * @return singleton admissionControlService Instance
     */
    public static AdmissionControlService getInstance() {
        return admissionControlService;
    }

    /**
     *
     * @param settings Immutable settings instance
     * @param clusterSettings ClusterSettings Instance
     * @param threadPool ThreadPool Instance
     * @return singleton admissionControlService Instance
     */
    public static synchronized AdmissionControlService newAdmissionControlService(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool
    ) {
        admissionControlService = new AdmissionControlService(settings, clusterSettings, threadPool);
        return admissionControlService;
    }

    /**
     *
     * @return true if the admissionController Feature is enabled
     */
    public Boolean isTransportLayerAdmissionControlEnabled() {
        return this.admissionControlSettings.isTransportLayerAdmissionControlEnabled();
    }

    /**
     *
     * @return true if the admissionController Feature is enabled
     */
    public Boolean isTransportLayerAdmissionControlEnforced() {
        return this.admissionControlSettings.isTransportLayerAdmissionControlEnforced();
    }

    /**
     *
     * @param admissionControllerName admissionControllerName to register into the service.
     */
    public void registerAdmissionController(String admissionControllerName) {
        AdmissionController admissionController = this.controllerFactory(admissionControllerName);
        if (admissionController != null) {
            this.ADMISSION_CONTROLLERS.put(admissionControllerName, admissionController);
        }
    }

    /**
     * @return AdmissionController Instance
     */
    private AdmissionController controllerFactory(String admissionControllerName) {
        switch (admissionControllerName) {
            case IO_BASED_ADMISSION_CONTROLLER:
                return new IOBasedAdmissionController(admissionControllerName, this.settings, this.clusterSettings);
            default:
                return null;
        }
    }

    /**
     *
     * @return list of the registered admissionControllers
     */
    public List<AdmissionController> getAdmissionControllers() {
        return new ArrayList<>(this.ADMISSION_CONTROLLERS.values());
    }

    /**
     *
     * @param controllerName name of the admissionController
     * @return instance of the AdmissionController Instance
     */
    public AdmissionController getAdmissionController(String controllerName) {
        if (this.ADMISSION_CONTROLLERS.containsKey(controllerName)) {
            return this.ADMISSION_CONTROLLERS.get(controllerName);
        }
        return null;
    }
}
