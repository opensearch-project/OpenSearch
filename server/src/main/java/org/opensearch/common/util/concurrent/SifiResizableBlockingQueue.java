/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.common.util.concurrent;

import java.util.concurrent.BlockingQueue;

/**
 * A size based queue wrapping another blocking queue to provide (somewhat relaxed) capacity checks.
 * Mainly makes sense to use with blocking queues that are unbounded to provide the ability to do
 * capacity verification.
 */
public class SifiResizableBlockingQueue<E> extends SizeBlockingQueue<E> {

    public SifiResizableBlockingQueue(BlockingQueue<E> queue, int capacity) {
        super(queue, capacity);
    }

    /**
     * resize the max capacity of the queue
     * @param capacity max capacity of the queue
     */
    public void resize(int capacity) {
        this.capacity = capacity;
    }
}
