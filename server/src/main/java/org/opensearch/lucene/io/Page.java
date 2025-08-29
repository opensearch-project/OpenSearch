/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.io;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicLong;

public class Page {

    private long size;
    private boolean original;
    private AtomicLong refCnt;
    private  Cleaner.Cleanable cleanable;
    private static final Cleaner cleaner = Cleaner.create();

    // Constructor for original page (cached in cache)
    public Page(long size) {
        this.size = size;
        this.original = true;
        this.refCnt = new AtomicLong(1);
        this.cleanable = null;
    }

    // Constructor for cloned pages (returned to readers)
    private Page(AtomicLong refCnt, long size, boolean original) {
        this.size = size;
        this.original = original;
        this.refCnt = refCnt;
        if (!original) {
            this.cleanable = cleaner.register(this, new PageCleanupAction(refCnt));
        } else {

        }
    }

    public long getSize() {
        return size;
    }

    public int getRefCount() {
        return refCnt.intValue();
    }

    //decRef
    public boolean decRef() {
        return refCnt.decrementAndGet() > 0;
    }

    public Page clone() {
        // Create retained slice (increments reference count)
        refCnt.incrementAndGet();
        return new Page(refCnt, size, false);
    }

    public boolean isOriginal() {
        return original;
    }

    public byte getByte(long curOffsetInPage) {
        if (curOffsetInPage >= size) {
            throw new IndexOutOfBoundsException("Trying to read from " + curOffsetInPage + " bytes out of " + size + " bytes");
        }
        return 0;//mock value
    }



    // Cleanup action that holds only the ByteBuf reference must never hold reference of Page object.
    private static class PageCleanupAction implements Runnable {

        private final AtomicLong refCnt;

        PageCleanupAction(AtomicLong refCnt) {
            this.refCnt = refCnt;
        }

        @Override
        public void run() {
            long prev;
            do {
                prev = refCnt.get();
                if (prev == 0) return;
            } while (!refCnt.compareAndSet(prev, prev - 1));
        }
    }

}
