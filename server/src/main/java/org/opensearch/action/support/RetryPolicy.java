/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;

import java.util.Iterator;

/**
 * Various RetryPolicies for performing retry.
 */
public abstract class RetryPolicy implements Iterable<TimeValue> {
    /**
     *  It provides exponential backoff between retries until it reaches maxDelayForRetry.
     *  It uses equal jitter scheme as it is being used for throttled exceptions.
     *  It will make random distribution and also guarantees a minimum delay.
     *
     * @param baseDelay BaseDelay for exponential Backoff
     * @param maxDelayForRetry MaxDelay that can be returned from backoff policy
     * @return A backoff policy with exponential backoff with equal jitter which can't return delay more than given max delay
     */
    public static RetryPolicy exponentialEqualJitterBackoff(int baseDelay, int maxDelayForRetry) {
        return new ExponentialEqualJitterBackoff(baseDelay, maxDelayForRetry);
    }

    /**
     *  It provides exponential backoff between retries until it reaches Integer.MAX_VALUE.
     *  It will make random distribution of delay.
     *
     * @param baseDelay BaseDelay for exponential Backoff
     * @return A backoff policy with exponential backoff with equal jitter which can't return delay more than given max delay
     */
    public static RetryPolicy exponentialBackoff(long baseDelay) {
        return new ExponentialBackoff(baseDelay);
    }

    private static class ExponentialEqualJitterBackoff extends RetryPolicy {
        private final int maxDelayForRetry;
        private final int baseDelay;

        private ExponentialEqualJitterBackoff(int baseDelay, int maxDelayForRetry) {
            this.maxDelayForRetry = maxDelayForRetry;
            this.baseDelay = baseDelay;
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new ExponentialEqualJitterBackoffIterator(baseDelay, maxDelayForRetry);
        }
    }

    private static class ExponentialEqualJitterBackoffIterator implements Iterator<TimeValue> {
        /**
         * Retry limit to avoids integer overflow issues.
         * Post this limit, max delay will be returned with Equal Jitter.
         *
         * NOTE: If the value is greater than 30, there can be integer overflow
         * issues during delay calculation.
         **/
        private final int RETRIES_TILL_JITTER_INCREASE = 30;

        /**
         * Exponential increase in delay will happen till it reaches maxDelayForRetry.
         * Once delay has exceeded maxDelayForRetry, it will return maxDelayForRetry only
         * and not increase the delay.
         */
        private final int maxDelayForRetry;
        private final int baseDelay;
        private int retriesAttempted;

        private ExponentialEqualJitterBackoffIterator(int baseDelay, int maxDelayForRetry) {
            this.baseDelay = baseDelay;
            this.maxDelayForRetry = maxDelayForRetry;
        }

        /**
         * There is not any limit for this BackOff.
         * This Iterator will always return back off delay.
         *
         * @return true
         */
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public TimeValue next() {
            int retries = Math.min(retriesAttempted, RETRIES_TILL_JITTER_INCREASE);
            int exponentialDelay = (int) Math.min((1L << retries) * baseDelay, maxDelayForRetry);
            retriesAttempted++;
            return TimeValue.timeValueMillis((exponentialDelay / 2) + Randomness.get().nextInt(exponentialDelay / 2 + 1));
        }
    }

    private static class ExponentialBackoff extends RetryPolicy {
        private final long baseDelay;

        private ExponentialBackoff(long baseDelay) {
            this.baseDelay = baseDelay;
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new ExponentialBackoffIterator(baseDelay);
        }
    }

    private static class ExponentialBackoffIterator implements Iterator<TimeValue> {
        /**
         * Current delay in exponential backoff
         */
        private long currentDelay;

        private ExponentialBackoffIterator(long baseDelay) {
            this.currentDelay = baseDelay;
        }

        /**
         * There is not any limit for this BackOff.
         * This Iterator will always return back off delay.
         *
         * @return true
         */
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public TimeValue next() {
            TimeValue delayToReturn = TimeValue.timeValueMillis(Randomness.get().nextInt(Math.toIntExact(currentDelay)) + 1);
            currentDelay = Math.min(2 * currentDelay, Integer.MAX_VALUE);
            return delayToReturn;
        }
    }

}
