/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.tasks;

import java.util.Map;

/**
 * An interface for a request that can be used to register a task manager task
*
* @opensearch.internal
*/
public interface ProtobufTaskAwareRequest {
    /**
     * Set a reference to task that caused this task to be run.
    */
    default void setParentTask(String parentTaskNode, long parentTaskId) {
        setParentTask(new ProtobufTaskId(parentTaskNode, parentTaskId));
    }

    /**
     * Set a reference to task that created this request.
    */
    void setParentTask(ProtobufTaskId taskId);

    /**
     * Get a reference to the task that created this request. Implementers should default to
    * {@link ProtobufTaskId#EMPTY_TASK_ID}, meaning "there is no parent".
    */
    ProtobufTaskId getParentTask();

    /**
     * Returns the task object that should be used to keep track of the processing of the request.
    */
    default ProtobufTask createTask(long id, String type, String action, ProtobufTaskId parentTaskId, Map<String, String> headers) {
        return new ProtobufTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    /**
     * Returns optional description of the request to be displayed by the task manager
    */
    default String getDescription() {
        return "";
    }
}
