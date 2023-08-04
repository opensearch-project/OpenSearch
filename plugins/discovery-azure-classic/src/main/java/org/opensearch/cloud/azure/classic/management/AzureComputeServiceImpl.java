/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cloud.azure.classic.management;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ServiceLoader;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.Builder;
import com.microsoft.windowsazure.core.DefaultBuilder;
import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.SpecialPermission;
import org.opensearch.cloud.azure.classic.AzureServiceRemoteException;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;

public class AzureComputeServiceImpl extends AbstractLifecycleComponent implements AzureComputeService {
    private static final Logger logger = LogManager.getLogger(AzureComputeServiceImpl.class);

    private final ComputeManagementClient client;
    private final String serviceName;

    public AzureComputeServiceImpl(Settings settings) {
        String subscriptionId = getRequiredSetting(settings, Management.SUBSCRIPTION_ID_SETTING);

        serviceName = getRequiredSetting(settings, Management.SERVICE_NAME_SETTING);
        String keystorePath = getRequiredSetting(settings, Management.KEYSTORE_PATH_SETTING);
        String keystorePassword = getRequiredSetting(settings, Management.KEYSTORE_PASSWORD_SETTING);
        KeyStoreType keystoreType = Management.KEYSTORE_TYPE_SETTING.get(settings);

        logger.trace("creating new Azure client for [{}], [{}]", subscriptionId, serviceName);
        try {
            // Azure SDK configuration uses DefaultBuilder which uses java.util.ServiceLoader to load the
            // various Azure services. By default, this will use the current thread's context classloader
            // to load services. Since the current thread refers to the main application classloader it
            // won't find any Azure service implementation.

            // Here we basically create a new DefaultBuilder that uses the current class classloader to load services.
            DefaultBuilder builder = new DefaultBuilder();
            for (Builder.Exports exports : ServiceLoader.load(Builder.Exports.class, getClass().getClassLoader())) {
                exports.register(builder);
            }

            // And create a new blank configuration based on the previous DefaultBuilder
            Configuration configuration = new Configuration(builder);
            configuration.setProperty(Configuration.PROPERTY_LOG_HTTP_REQUESTS, logger.isTraceEnabled());

            Configuration managementConfig = ManagementConfiguration.configure(
                null,
                configuration,
                Management.ENDPOINT_SETTING.get(settings),
                subscriptionId,
                keystorePath,
                keystorePassword,
                keystoreType
            );

            logger.debug("creating new Azure client for [{}], [{}]", subscriptionId, serviceName);
            client = ComputeManagementService.create(managementConfig);
        } catch (IOException e) {
            throw new OpenSearchException("Unable to configure Azure compute service", e);
        }
    }

    private static String getRequiredSetting(Settings settings, Setting<String> setting) {
        String value = setting.get(settings);
        if (value == null || Strings.hasLength(value) == false) {
            throw new IllegalArgumentException("Missing required setting " + setting.getKey() + " for azure");
        }
        return value;
    }

    @Override
    public HostedServiceGetDetailedResponse getServiceDetails() {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<HostedServiceGetDetailedResponse>) () -> client.getHostedServicesOperations()
                    .getDetailed(serviceName)
            );
        } catch (PrivilegedActionException e) {
            throw new AzureServiceRemoteException("can not get list of azure nodes", e.getCause());
        }
    }

    @Override
    protected void doStart() throws OpenSearchException {}

    @Override
    protected void doStop() throws OpenSearchException {}

    @Override
    protected void doClose() throws OpenSearchException {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                logger.error("error while closing Azure client", e);
            }
        }
    }
}
