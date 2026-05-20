/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.Writeable;

/**
 * Metadata node produced by the planner (via backend's factory) at the coordinator
 * and consumed by the backend's handler at the data node. Carries typed configuration
 * that the handler uses to configure the execution environment.
 *
 * <p>Generic parent interface — backends extend with concrete classes if they need
 * additional coordinator-side context beyond what the framework provides.
 *
 * @opensearch.internal
 */
public interface InstructionNode extends Writeable {

    /** The instruction type — used to look up the handler factory at the data node. */
    InstructionType type();
}
