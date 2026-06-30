/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.worker;

import java.util.Objects;

/**
 * Represents a worker node in the fleet
 */
public class WorkerNode {
    /**
     * The unique identifier of the worker node.
     */
    private final String id;

    /**
     * The name of the worker node.
     */
    private final String name;

    /**
     * The IP address of the worker node.
     */
    private final String ip;

    /**
     * Creates a new worker node with the given ID, name, and IP address.
     *
     * @param id The unique identifier of the worker node.
     * @param name The name of the worker node.
     * @param ip The IP address of the worker node.
     */
    private WorkerNode(String id, String name, String ip) {
        this.id = id;
        this.name = name;
        this.ip = ip;
    }

    /**
     * Creates a new worker node with the given ID, name, and IP address.
     *
     * @param id The unique identifier of the worker node.
     * @param name The name of the worker node.
     * @param ip The IP address of the worker node.
     * @return The created worker node.
     */
    public static WorkerNode createWorkerNode(String id, String name, String ip) {
        return new WorkerNode(id, name, ip);
    }

    /**
     * Returns the unique identifier of the worker node.
     *
     * @return The ID of the worker node.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the name of the worker node.
     *
     * @return The name of the worker node.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the IP address of the worker node.
     *
     * @return The IP address of the worker node.
     */
    public String getIp() {
        return ip;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerNode that = (WorkerNode) o;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name) && Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, ip);
    }
}
