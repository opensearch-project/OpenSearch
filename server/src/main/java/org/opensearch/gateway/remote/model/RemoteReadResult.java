/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Container class for entity read from remote store
 */
@ExperimentalApi
public class RemoteReadResult<T> {

    T obj;
    String component;
    String componentName;
    /**
     * time spent in ms in reading the blob from blob store
     */
    final long readMS;
    /**
     * time spent in ms during deserializing the response
     */
    final long serdeMS;

    public RemoteReadResult(T obj, String component, String componentName, long readMS, long serdeMS) {
        this.obj = obj;
        this.component = component;
        this.componentName = componentName;
        this.readMS = readMS;
        this.serdeMS = serdeMS;
    }

    public T getObj() {
        return obj;
    }

    public String getComponent() {
        return component;
    }

    public String getComponentName() {
        return componentName;
    }

    public long getReadMS() {
        return readMS;
    }

    public long getSerdeMS() {
        return serdeMS;
    }
}
