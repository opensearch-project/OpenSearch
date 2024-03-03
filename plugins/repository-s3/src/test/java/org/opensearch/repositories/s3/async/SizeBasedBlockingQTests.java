/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.s3.S3TransferRejectedException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SizeBasedBlockingQTests extends OpenSearchTestCase {
    private ExecutorService consumerService;
    private ExecutorService producerService;

    @Override
    @Before
    public void setUp() throws Exception {
        this.consumerService = Executors.newFixedThreadPool(10);
        this.producerService = Executors.newFixedThreadPool(100);
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        consumerService.shutdown();
        producerService.shutdown();
        super.tearDown();
    }

    public void testProducerConsumerOfBulkItems() throws InterruptedException {

        SizeBasedBlockingQ sizeBasedBlockingQ = new SizeBasedBlockingQ(
            new ByteSizeValue(ByteSizeUnit.BYTES.toBytes(10)),
            consumerService,
            10
        );
        sizeBasedBlockingQ.start();
        int numOfItems = randomIntBetween(100, 1000);
        CountDownLatch latch = new CountDownLatch(numOfItems);
        AtomicBoolean unknownError = new AtomicBoolean();
        for (int i = 0; i < numOfItems; i++) {
            final int idx = i;
            producerService.submit(() -> {
                boolean throwException = randomBoolean();

                SizeBasedBlockingQ.Item item = new TestItemToStr(randomIntBetween(1, 5), () -> {
                    latch.countDown();
                    if (throwException) {
                        throw new RuntimeException("throwing random exception");
                    }
                }, idx);

                try {
                    sizeBasedBlockingQ.produce(item);
                } catch (InterruptedException e) {
                    latch.countDown();
                    unknownError.set(true);
                    throw new RuntimeException(e);
                } catch (S3TransferRejectedException ex) {
                    latch.countDown();
                }
            });
        }
        latch.await();
        sizeBasedBlockingQ.close();
        assertFalse(unknownError.get());
    }

    static class TestItemToStr extends SizeBasedBlockingQ.Item {
        private final int id;

        public TestItemToStr(long size, Runnable consumable, int id) {
            super(size, consumable);
            this.id = id;
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }
    }
}
