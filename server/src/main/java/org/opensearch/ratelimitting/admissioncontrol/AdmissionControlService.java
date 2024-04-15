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
import org.apache.lucene.util.Constants;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.IoBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControlStats;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControllerStats;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER;
import static org.opensearch.ratelimitting.admissioncontrol.controllers.IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER;

/**
 * Admission control Service that bootstraps and manages all the Admission Controllers in OpenSearch.
 */
public class AdmissionControlService {
    private final ThreadPool threadPool;
    public final AdmissionControlSettings admissionControlSettings;
    private final ConcurrentMap<String, AdmissionController> admissionControllers;
    private static final Logger logger = LogManager.getLogger(AdmissionControlService.class);
    private final ClusterService clusterService;
    private final Settings settings;
    private final ResourceUsageCollectorService resourceUsageCollectorService;

    /**
     *
     * @param settings Immutable settings instance
     * @param clusterService ClusterService Instance
     * @param threadPool ThreadPool Instance
     * @param resourceUsageCollectorService Instance used to get node resource usage stats
     */
    public AdmissionControlService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceUsageCollectorService resourceUsageCollectorService
    ) {
        this.threadPool = threadPool;
        this.admissionControlSettings = new AdmissionControlSettings(clusterService.getClusterSettings(), settings);
        this.admissionControllers = new ConcurrentHashMap<>();
        this.clusterService = clusterService;
        this.settings = settings;
        this.resourceUsageCollectorService = resourceUsageCollectorService;
        this.initialize();
    }

    /**
     * Initialise and Register all the admissionControllers
     */
    private void initialize() {
        // Initialise different type of admission controllers
        registerAdmissionController(CPU_BASED_ADMISSION_CONTROLLER);
        if (Constants.LINUX) {
            registerAdmissionController(IO_BASED_ADMISSION_CONTROLLER);
        }
    }

    /**
     *
     * @param action Transport action name
     * @param admissionControlActionType admissionControllerActionType value
     */
    public void applyTransportAdmissionControl(String action, AdmissionControlActionType admissionControlActionType) {
        this.admissionControllers.forEach(
            (name, admissionController) -> { admissionController.apply(action, admissionControlActionType); }
        );
    }

    /**
     *
     * @param admissionControllerName admissionControllerName to register into the service.
     */
    public void registerAdmissionController(String admissionControllerName) {
        AdmissionController admissionController = this.controllerFactory(admissionControllerName);
        this.admissionControllers.put(admissionControllerName, admissionController);
    }

    /**
     * @return AdmissionController Instance
     */
    private AdmissionController controllerFactory(String admissionControllerName) {
        switch (admissionControllerName) {
            case CPU_BASED_ADMISSION_CONTROLLER:
                return new CpuBasedAdmissionController(
                    admissionControllerName,
                    this.resourceUsageCollectorService,
                    this.clusterService,
                    this.settings
                );
            case IO_BASED_ADMISSION_CONTROLLER:
                return new IoBasedAdmissionController(
                    admissionControllerName,
                    this.resourceUsageCollectorService,
                    this.clusterService,
                    this.settings
                );
            default:
                throw new IllegalArgumentException("Not Supported AdmissionController : " + admissionControllerName);
        }
    }

    /**
     *
     * @return list of the registered admissionControllers
     */
    public List<AdmissionController> getAdmissionControllers() {
        return new ArrayList<>(this.admissionControllers.values());
    }

    /**
     *
     * @param controllerName name of the admissionController
     * @return instance of the AdmissionController Instance
     */
    public AdmissionController getAdmissionController(String controllerName) {
        return this.admissionControllers.getOrDefault(controllerName, null);
    }

    /**
     * Return admission control stats
     */
    public AdmissionControlStats stats() {
        List<AdmissionControllerStats> statsList = new ArrayList<>();
        if (!this.admissionControllers.isEmpty()) {
            this.admissionControllers.forEach((controllerName, admissionController) -> {
                AdmissionControllerStats admissionControllerStats = new AdmissionControllerStats(admissionController);
                statsList.add(admissionControllerStats);
            });
            return new AdmissionControlStats(statsList);
        }
        return null;
    }
}
