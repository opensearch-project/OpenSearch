/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc.services;

import io.grpc.BindableService;

import java.util.ArrayList;

public class gRPCServiceRegistry {
    private final ArrayList<BindableService> services = new ArrayList<>();

    gRPCServiceRegistry() { }

    public void addService(BindableService bindableService) {
        services.add(bindableService);
    }

    public ArrayList<BindableService> getServices() {
        return services;
    }
}
