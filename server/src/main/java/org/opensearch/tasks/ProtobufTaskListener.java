/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.tasks;

/**
 * Listener for Task success or failure.
*
* @opensearch.internal
*/
public interface ProtobufTaskListener<Response> {
    /**
     * Handle task response. This response may constitute a failure or a success
    * but it is up to the listener to make that decision.
    *
    * @param task
    *            the task being executed. May be null if the action doesn't
    *            create a task
    * @param response
    *            the response from the action that executed the task
    */
    void onResponse(ProtobufTask task, Response response);

    /**
     * A failure caused by an exception at some phase of the task.
    *
    * @param task
    *            the task being executed. May be null if the action doesn't
    *            create a task
    * @param e
    *            the failure
    */
    void onFailure(ProtobufTask task, Exception e);

}
