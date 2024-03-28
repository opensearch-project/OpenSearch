/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Queue implementation to accept events based on their storage attribute. Rest of the behaviour gets inherited
 * from blocking queue where exceeding size blocks the main thread.
 */
public class SizeBasedBlockingQ extends AbstractLifecycleComponent {
    private static final Logger log = LogManager.getLogger(SizeBasedBlockingQ.class);

    protected final LinkedBlockingQueue<Item> queue;
    protected final Lock lock;
    protected final Condition notFull;
    protected final Condition notEmpty;

    protected final AtomicLong currentSize;
    protected final ByteSizeValue capacity;
    protected final AtomicBoolean closed;
    protected final ExecutorService executorService;
    protected final int consumers;

    /**
     * Constructor to create sized based blocking queue.
     */
    public SizeBasedBlockingQ(ByteSizeValue capacity, ExecutorService executorService, int consumers) {
        this.queue = new LinkedBlockingQueue<>();
        this.lock = new ReentrantLock();
        this.notFull = lock.newCondition();
        this.notEmpty = lock.newCondition();
        this.currentSize = new AtomicLong();
        this.capacity = capacity;
        this.closed = new AtomicBoolean();
        this.executorService = executorService;
        this.consumers = consumers;
    }

    @Override
    protected void doStart() {
        for (int worker = 0; worker < consumers; worker++) {
            Thread consumer = new Consumer(queue, currentSize, lock, notFull, notEmpty, closed);
            executorService.submit(consumer);
        }
    }

    /**
     * Add an item to the queue
     */
    public void produce(Item item) throws InterruptedException {
        if (item == null || item.size <= 0) {
            throw new IllegalStateException("Invalid item input to produce.");
        }
        final Lock lock = this.lock;
        final AtomicLong currentSize = this.currentSize;
        lock.lock();
        try {
            if (closed.get()) {
                throw new AlreadyClosedException("Transfer queue is already closed.");
            }
            while (currentSize.get() + item.size >= capacity.getBytes()) {
                notFull.await();
            }
            if (closed.get()) {
                throw new AlreadyClosedException("Transfer queue is already closed.");
            }
            queue.put(item);
            currentSize.addAndGet(item.size);
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int getSize() {
        return queue.size();
    }

    protected static class Consumer extends Thread {
        private final LinkedBlockingQueue<Item> queue;
        private final Lock lock;
        private final Condition notFull;
        private final Condition notEmpty;
        private final AtomicLong currentSize;
        private final AtomicBoolean closed;

        public Consumer(
            LinkedBlockingQueue<Item> queue,
            AtomicLong currentSize,
            Lock lock,
            Condition notFull,
            Condition notEmpty,
            AtomicBoolean closed
        ) {
            this.queue = queue;
            this.lock = lock;
            this.notEmpty = notEmpty;
            this.notFull = notFull;
            this.currentSize = currentSize;
            this.closed = closed;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    consume();
                } catch (AlreadyClosedException ex) {
                    return;
                } catch (Exception ex) {
                    log.error("Failed to consume transfer event", ex);
                }
            }
        }

        private void consume() throws InterruptedException {
            final Lock lock = this.lock;
            final AtomicLong currentSize = this.currentSize;
            lock.lock();
            Item item;
            try {
                if (closed.get()) {
                    throw new AlreadyClosedException("transfer queue closed");
                }
                while (currentSize.get() == 0) {
                    notEmpty.await();
                    if (closed.get()) {
                        throw new AlreadyClosedException("transfer queue closed");
                    }
                }

                item = queue.take();
                currentSize.addAndGet(-item.size);
                notFull.signalAll();
            } finally {
                lock.unlock();
            }

            try {
                item.consumable.run();
            } catch (Exception ex) {
                log.error("Exception on executing item consumable", ex);
            }
        }

    }

    public static class Item {
        private final long size;
        private final Runnable consumable;

        public Item(long size, Runnable consumable) {
            this.size = size;
            this.consumable = consumable;
        }
    }

    @Override
    protected void doStop() {
        doClose();
    }

    @Override
    protected void doClose() {
        lock.lock();
        if (closed.get() == true) {
            return;
        }
        closed.set(true);
        try {
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
