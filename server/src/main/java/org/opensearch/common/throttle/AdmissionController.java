/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.throttle;

import org.opensearch.common.lease.Releasable;

/**
 * Aims to provide resource based request admission control.
 * Provides methods for any tracking-object that can be incremented (such as memory size),
 * Admission control can be applied if configured limit has been reached.
 */
public interface AdmissionController {

    /**
     * @return name of the admission-controller
     */
    String getName();

    /**
     * @return admission-controller enabled status
     */
    boolean isEnabled();

    /**
     * Increment the tracking-object with provided value.
     * Apply the admission control if threshold is breached.
     * Mostly applicable while acquiring the quota.
     * Later Releasable is used to decrement the tracking-object with previously acquired value.
     *
     * @param count value to incrementation the resource racking-object with.
     * @return Releasable for tokens acquired from the resource tracking object.
     */
    Releasable addBytesAndMaybeBreak(long count);

    /**
     * Sets the initial used quota for the controller. Primarily used when copying controller states.
     * @param count To set the value of the tracking resource object as the provided count
     * @return count/value by which the resource tracking object is updated with.
     */
    long setInitialQuota(long count);

    /**
     * @return limit defined for the tracking-object being tracked by the admission-controller.
     */
    long getTotalQuota();

    /**
     * @return currently acquired value of the tracking-object being tracked by the admission-controller.
     */
    long getUsedQuota();

    /**
     * @return current value of the rejection count metric tracked by the admission-controller.
     */
    long getRejectionCount();

    /**
     * Adds the rejection count for the controller. Primarily used when copying controller states.
     * @param count To add the value of the tracking resource object as the provided count
     * @return count/value by which the resource tracking object is updated with.
     */
    long addRejectionCount(long count);
}
