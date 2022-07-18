/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.throttle;

import org.opensearch.common.lease.Releasable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * RequestSizeAdmissionController tracks the memory quota in bytes allocated for a request object based upon its
 * request-size (content-length)
 *
 * @opensearch.internal
 */
class RequestSizeAdmissionController implements AdmissionController {
    private final AdmissionControlSetting admissionControlSetting;
    private final AtomicLong usedBytes;
    private final AtomicLong rejectionCount;

    /**
     * @param admissionControlSetting setting for request-size admission-controller
     */
    public RequestSizeAdmissionController(AdmissionControlSetting admissionControlSetting) {
        this.admissionControlSetting = admissionControlSetting;
        this.usedBytes = new AtomicLong(0);
        this.rejectionCount = new AtomicLong(0);
    }

    /**
     * @return name of the admission-controller
     */
    @Override
    public String getName() {
        return this.admissionControlSetting.getName();
    }

    /**
     * @return admission-controller enabled status
     */
    @Override
    public boolean isEnabled() {
        return this.admissionControlSetting.isEnabled();
    }

    /**
     * Sets the initial used quota for the controller. Primarily used when copying controller states.
     * @param count To set the value of the tracking resource object as the provided count
     * @return count/value by which the resource tracking object is updated with.
     */
    @Override
    public long setInitialQuota(long count) {
        usedBytes.set(count);
        return usedBytes.get();
    }

    /**
     * @return currently acquired value of the tracking-object being tracked by the admission-controller.
     */
    @Override
    public long getUsedQuota() {
        return usedBytes.get();
    }

    /**
     * @return current value of the rejection count metric tracked by the admission-controller.
     */
    @Override
    public long getRejectionCount() {
        return this.rejectionCount.get();
    }

    /**
     * Adds the rejection count for the controller. Primarily used when copying controller states.
     * @param count To add the value of the tracking resource object as the provided count
     * @return count/value by which the resource tracking object is updated with.
     */
    @Override
    public long addRejectionCount(long count) {
        return this.rejectionCount.addAndGet(count);
    }

    /**
     * Increments the memory-tracking object for request-size quota with the provided bytes; and apply the admission
     * control, if threshold is breached.
     * Expected to be used while acquiring the quota.
     *
     * @param bytes byte size value to add to the current memory-tracking object and verify the threshold.
     * @return byte size value used for comparison, if controller is enabled; zero otherwise
     * @throws IllegalStateException if current memory usage quota along with requested bytes is greater than
     * pre-defined threshold.
     */
    @Override
    public Releasable addBytesAndMaybeBreak(long bytes) {
        // todo: how the request should be allowed or throttled
        return () -> {};
    }
}
