/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.IndexInput;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.hasSize;

/**
 * Unit test to ensure that {@link OnDemandBlockIndexInput} properly closes
 * all of its backing IndexInput instances, as the reference counting logic
 * relies on this behavior.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class OnDemandBlockIndexInputLifecycleTests extends OpenSearchTestCase {
    private static final int ONE_MB_SHIFT = 20;
    private static final int ONE_MB = 1 << ONE_MB_SHIFT;
    private static final int TWO_MB = ONE_MB * 2;

    private final List<CloseTrackingIndexInput> allIndexInputs = new ArrayList<>();

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        assertBusy(() -> {
            System.gc(); // Do not rely on GC to be deterministic, hence the polling
            assertTrue("Expected all IndexInputs to be closed", allIndexInputs.stream().allMatch(CloseTrackingIndexInput::isClosed));
        }, 5, TimeUnit.SECONDS);
    }

    public void testClose() throws IOException {
        try (OnDemandBlockIndexInput indexInput = createTestOnDemandBlockIndexInput()) {
            indexInput.seek(0);
        }
    }

    public void testCloseWhenSeekingMultipleChunks() throws IOException {
        try (OnDemandBlockIndexInput indexInput = createTestOnDemandBlockIndexInput()) {
            indexInput.seek(0);
            indexInput.seek(ONE_MB + 1);
        }
        MatcherAssert.assertThat("Expected to seek past first block and create a second block", allIndexInputs, hasSize(2));
    }

    public void testUnclosedCloneIsClosed() throws IOException {
        try (OnDemandBlockIndexInput indexInput = createTestOnDemandBlockIndexInput()) {
            indexInput.seek(0);

            // Clone is abandoned without closing
            indexInput.clone().seek(0);
        }
    }

    public void testUnclosedSliceIsClosed() throws IOException {
        try (OnDemandBlockIndexInput indexInput = createTestOnDemandBlockIndexInput()) {
            indexInput.seek(0);

            // Clone is abandoned without closing
            indexInput.slice("slice", 0, 100).seek(0);
        }
    }

    private OnDemandBlockIndexInput createTestOnDemandBlockIndexInput() {
        return new TestOnDemandBlockIndexInput(this::createCloseTrackingIndexInput, false);
    }

    private IndexInput createCloseTrackingIndexInput() {
        final CloseTrackingIndexInput i = new CloseTrackingIndexInput();
        allIndexInputs.add(i);
        return i;
    }

    /**
     * Concrete implementation of {@link OnDemandBlockIndexInput} that creates
     * {@link CloseTrackingIndexInput} index inputs when it needs to fetch a
     * new block.
     */
    private static class TestOnDemandBlockIndexInput extends OnDemandBlockIndexInput {
        private final Supplier<IndexInput> indexInputSupplier;

        TestOnDemandBlockIndexInput(Supplier<IndexInput> indexInputSupplier, boolean isClone) {
            super(
                builder().blockSizeShift(ONE_MB_SHIFT)
                    .offset(0)
                    .length(TWO_MB)
                    .isClone(isClone)
                    .resourceDescription(TestOnDemandBlockIndexInput.class.getName())
            );
            this.indexInputSupplier = indexInputSupplier;
        }

        @Override
        protected OnDemandBlockIndexInput buildSlice(String sliceDescription, long offset, long length) {
            return new TestOnDemandBlockIndexInput(this.indexInputSupplier, true);
        }

        @Override
        protected IndexInput fetchBlock(int blockId) throws IOException {
            return indexInputSupplier.get();
        }

        @Override
        public OnDemandBlockIndexInput clone() {
            return new TestOnDemandBlockIndexInput(this.indexInputSupplier, true);
        }
    }

    /**
     * Simple implementation of an IndexInput that just tracks whether it has
     * been closed. All other methods do nothing useful.
     */
    private static class CloseTrackingIndexInput extends IndexInput {

        private boolean isClosed = false;

        protected CloseTrackingIndexInput() {
            super("TestIndexInput");
        }

        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public long getFilePointer() {
            return 0;
        }

        @Override
        public void seek(long pos) {}

        @Override
        public long length() {
            return 0;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            return null;
        }

        @Override
        public byte readByte() throws IOException {
            return 0;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) {}
    }
}
