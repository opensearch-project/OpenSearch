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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CPUBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControlStats;
import org.opensearch.ratelimitting.admissioncontrol.stats.BaseAdmissionControllerStats;
import org.opensearch.ratelimitting.admissioncontrol.stats.CPUBasedAdmissionControllerStats;
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
    private final ClusterService clusterService;
    private final Settings settings;

    private ResourceUsageCollectorService resourceUsageCollectorService;

    /**
     *
     * @param settings Immutable settings instance
     * @param clusterService ClusterService Instance
     * @param threadPool ThreadPool Instance
     */
    public AdmissionControlService(Settings settings, ClusterService clusterService, ThreadPool threadPool, ResourceUsageCollectorService resourceUsageCollectorService) {
        this.threadPool = threadPool;
        this.admissionControlSettings = new AdmissionControlSettings(clusterService.getClusterSettings(), settings);
        this.ADMISSION_CONTROLLERS = new ConcurrentHashMap<>();
        this.clusterService = clusterService;
        this.settings = settings;
        this.resourceUsageCollectorService = resourceUsageCollectorService;
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
     *
     * @param action transport action that is being executed. we are using it for logging while request is rejected
     * @param admissionControlActionType type of the admissionControllerActionType
     */
    public void applyTransportAdmissionControl(String action, AdmissionControlActionType admissionControlActionType) {
        this.ADMISSION_CONTROLLERS.forEach((name, admissionController) -> { admissionController.apply(action, admissionControlActionType); });
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
                return new CPUBasedAdmissionController(admissionControllerName, this.settings, this.clusterService, this.resourceUsageCollectorService);
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

    public AdmissionControlStats stats(){
        List<BaseAdmissionControllerStats> statsList = new ArrayList<>();
        if(this.ADMISSION_CONTROLLERS.size() > 0){
            this.ADMISSION_CONTROLLERS.forEach((controllerName, admissionController) -> {
                BaseAdmissionControllerStats admissionControllerStats = controllerStatsFactory(admissionController);
                if(admissionControllerStats != null) {
                    statsList.add(admissionControllerStats);
                }
            });
            return new AdmissionControlStats(statsList);
        }
        return null;
    }

    private BaseAdmissionControllerStats controllerStatsFactory(AdmissionController admissionController) {
        switch (admissionController.getName()) {
            case CPU_BASED_ADMISSION_CONTROLLER:
                return new CPUBasedAdmissionControllerStats(admissionController);
            default:
                return null;
        }
    }
}
