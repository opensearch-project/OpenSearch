/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Represents the state of a deployment operation.
 *
 * @opensearch.internal
 */
public enum DeploymentState implements Writeable {
    DRAIN,
    FINISH;

    public static DeploymentState fromString(String state) {
        return valueOf(state.toUpperCase(Locale.ROOT));
    }

    public static DeploymentState readFrom(StreamInput in) throws IOException {
        return fromString(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name());
    }
}
