/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

/**
 * Container class for entity read from remote store
 */
public class RemoteReadResult {

    Object obj;
    String component;
    String componentName;

    public RemoteReadResult(Object obj, String component, String componentName) {
        this.obj = obj;
        this.component = component;
        this.componentName = componentName;
    }

    public Object getObj() {
        return obj;
    }

    public String getComponent() {
        return component;
    }

    public String getComponentName() {
        return componentName;
    }
}
