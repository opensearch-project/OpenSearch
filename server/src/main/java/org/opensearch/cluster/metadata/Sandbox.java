/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;

import java.util.Collections;
import java.util.List;

/**
 * Stub class only, this will be added in Kaushal's PR
 * This class wil be deleted after that.
 */
@ExperimentalApi
public class Sandbox {
    // TODO Kaushal should have implemented hashcode and equals
    private SandboxMode mode;

    public SandboxMode getMode() {
        return mode;
    }

    public ResourceLimit getResourceLimitFor(SandboxResourceType resourceType) {
        return null;
    }

    public String getName() {
        return "";
    }

    public String getId() {
        return "";
    }

    public List<ResourceLimit> getResourceLimits() {
        return Collections.emptyList();
    }

    /**
     * Stub class only, this will be added in Kaushal's PR
     * This class wil be deleted after that.
     */
    @ExperimentalApi
    public class ResourceLimit {
        /*
        Stub class only
        */
        public Long getThresholdInLong() {
            return 0L;
        }

        public SandboxResourceType getResourceType() {
            return null;
        }

        public Long getThreshold() {
            return 0L;
        }
    }

    /**
     * Stub class only, this will be added in Kaushal's PR
     * This class wil be deleted after that.
     */
    @ExperimentalApi
    public enum SandboxMode {
        SOFT("soft"),
        ENFORCED("enforced"),
        MONITOR("monitor");

        private final String name;

        SandboxMode(String mode) {
            this.name = mode;
        }

        public String getName() {
            return name;
        }

        public static SandboxMode fromName(String s) {
            switch (s) {
                case "soft":
                    return SOFT;
                case "enforced":
                    return ENFORCED;
                case "monitor":
                    return MONITOR;
                default:
                    throw new IllegalArgumentException("Invalid value for SandboxMode: " + s);
            }
        }
    }
}
