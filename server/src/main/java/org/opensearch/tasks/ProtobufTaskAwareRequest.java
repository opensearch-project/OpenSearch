/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
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
    default void setProtobufParentTask(String parentTaskNode, long parentTaskId) {
        setProtobufParentTask(new ProtobufTaskId(parentTaskNode, parentTaskId));
    }

    /**
     * Set a reference to task that created this request.
    */
    void setProtobufParentTask(ProtobufTaskId taskId);

    /**
     * Get a reference to the task that created this request. Implementers should default to
    * {@link ProtobufTaskId#EMPTY_TASK_ID}, meaning "there is no parent".
    */
    ProtobufTaskId getProtobufParentTask();

    /**
     * Returns the task object that should be used to keep track of the processing of the request.
    */
    default ProtobufTask createProtobufTask(long id, String type, String action, ProtobufTaskId parentTaskId, Map<String, String> headers) {
        System.out.println("Creating protobuf task");
        System.out.println("id: " + id);
        System.out.println("type: " + type);
        System.out.println("action: " + action);
        System.out.println("parentTaskId: " + parentTaskId);
        System.out.println("headers: " + headers);
        return new ProtobufTask(id, type, action, getTaskDescription(), parentTaskId, headers);
    }

    /**
     * Returns optional description of the request to be displayed by the task manager
    */
    default String getTaskDescription() {
        return "";
    }
}
