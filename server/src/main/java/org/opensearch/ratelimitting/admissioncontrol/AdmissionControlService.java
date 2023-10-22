/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CPUBasedAdmissionController;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.opensearch.ratelimitting.admissioncontrol.settings.CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER;

/**
 * Admission control Service that bootstraps and manages all the Admission Controllers in OpenSearch.
 */
public class AdmissionControlService {
    private final ThreadPool threadPool;
    public final AdmissionControlSettings admissionControlSettings;
    private final ConcurrentMap<String, AdmissionController> ADMISSION_CONTROLLERS;
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
    private void initialise() {
        // Initialise different type of admission controllers
        registerAdmissionController(CPU_BASED_ADMISSION_CONTROLLER);
    }

    /**
     * Handler to trigger registered admissionController
     */
    public void applyTransportAdmissionControl(String action) {
        this.ADMISSION_CONTROLLERS.forEach((name, admissionController) -> { admissionController.apply(action); });
    }

    /**
     *
     * @param admissionControllerName admissionControllerName to register into the service.
     */
    public void registerAdmissionController(String admissionControllerName) {
        AdmissionController admissionController = this.controllerFactory(admissionControllerName);
        this.ADMISSION_CONTROLLERS.put(admissionControllerName, admissionController);
    }

    /**
     * @return AdmissionController Instance
     */
    private AdmissionController controllerFactory(String admissionControllerName) {
        switch (admissionControllerName) {
            case CPU_BASED_ADMISSION_CONTROLLER:
                return new CPUBasedAdmissionController(admissionControllerName, this.settings, this.clusterSettings);
            default:
                throw new IllegalArgumentException("Not Supported AdmissionController : " + admissionControllerName);
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
        return this.ADMISSION_CONTROLLERS.getOrDefault(controllerName, null);
    }
}
