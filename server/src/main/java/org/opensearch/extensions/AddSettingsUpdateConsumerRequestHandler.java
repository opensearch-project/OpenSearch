/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.settings.WriteableSetting;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

/**
 * Handles requests to add setting update consumers
 *
 * @opensearch.internal
 */
public class AddSettingsUpdateConsumerRequestHandler {

    private static final Logger logger = LogManager.getLogger(AddSettingsUpdateConsumerRequestHandler.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final String updateSettingsRequestType;
    private final SettingsModule settingsModule;

    /**
     * Instantiates a new Add Settings Update Consumer Request Handler with the {@link ClusterService} and {@link TransportService}
     *
     * @param clusterService the cluster service used to extract cluster settings
     * @param transportService the node's transport service
     * @param updateSettingsRequestType the update settings request type
     */
    public AddSettingsUpdateConsumerRequestHandler(
        ClusterService clusterService,
        TransportService transportService,
        String updateSettingsRequestType,
        SettingsModule settingsModule
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.updateSettingsRequestType = updateSettingsRequestType;
        this.settingsModule = settingsModule;
    }

    /**
     * Handles a {@link AddSettingsUpdateConsumerRequest}.
     *
     * @param addSettingsUpdateConsumerRequest  The request to handle.
     * @return  A {@link AcknowledgedResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    TransportResponse handleAddSettingsUpdateConsumerRequest(AddSettingsUpdateConsumerRequest addSettingsUpdateConsumerRequest)
        throws Exception {

        boolean status = true;
        List<WriteableSetting> extensionComponentSettings = addSettingsUpdateConsumerRequest.getComponentSettings();
        DiscoveryExtensionNode extensionNode = addSettingsUpdateConsumerRequest.getExtensionNode();

        try {
            for (WriteableSetting extensionComponentSetting : extensionComponentSettings) {

                // Extract setting and type from writeable setting
                Setting<?> setting = extensionComponentSetting.getSetting();

                // we need to get the actual setting from nodeSetting or indexsetting maps in SettingsModule
                // use conditional based on setting properties
                Setting<?> settingForUpdateConsumer = null;
                if (setting.hasIndexScope()) {
                    settingForUpdateConsumer = settingsModule.getIndexScopedSettings().get(setting.getKey());
                } else if (setting.hasNodeScope()) {
                    settingForUpdateConsumer = settingsModule.getClusterSettings().get(setting.getKey());
                }
                // do a null check and throw IllegalArgument exception here if neither index or node scope
                if (settingForUpdateConsumer == null) {
                    throw new IllegalArgumentException("Not index or node scope");
                }

                WriteableSetting.SettingType settingType = extensionComponentSetting.getType();

                if (setting.hasIndexScope()) {
                    // Register setting update consumer with callback method to extension
                    clusterService.getClusterSettings().addSettingsUpdateConsumer(settingForUpdateConsumer, (data) -> {
                        logger.debug("Sending extension request type: " + updateSettingsRequestType);
                        UpdateSettingsResponseHandler updateSettingsResponseHandler = new UpdateSettingsResponseHandler();
                        transportService.sendRequest(
                            extensionNode,
                            updateSettingsRequestType,
                            new UpdateSettingsRequest(settingType, setting, data),
                            updateSettingsResponseHandler
                        );
                    });
                }
                if (setting.hasNodeScope()) {
                    // Register setting update consumer with callback method to extension
                    clusterService.getClusterSettings().addSettingsUpdateConsumer(settingForUpdateConsumer, (data) -> {
                        logger.debug("Sending extension request type: " + updateSettingsRequestType);
                        UpdateSettingsResponseHandler updateSettingsResponseHandler = new UpdateSettingsResponseHandler();
                        transportService.sendRequest(
                            extensionNode,
                            updateSettingsRequestType,
                            new UpdateSettingsRequest(settingType, setting, data),
                            updateSettingsResponseHandler
                        );
                    });
                }
            }
        } catch (SettingsException | IllegalArgumentException e) {
            logger.error(e.toString());
            status = false;
        }
        return new AcknowledgedResponse(status);
    }
}
