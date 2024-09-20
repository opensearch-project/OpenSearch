/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.task;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Base class for all TaskParams implementation of various TaskTypes
 */
@ExperimentalApi
public abstract class TaskParams {

    /**
     * Default constructor
     */
    public TaskParams() {}
}
