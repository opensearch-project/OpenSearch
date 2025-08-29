/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.ReadAdvice;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;

public class IOInterceptingIndexInput extends IndexInput implements RandomAccessInput {

    private static final Logger logger = LogManager.getLogger(IOInterceptingIndexInput.class);

    protected final BufferCache bufferCache;
    protected final String name;
    // slice length same as fileLength in case of pure files
    protected final IndexInput delegate;
    protected final IOContext context;
    protected final boolean isClone;
    protected final long off; // start offset: non-zero in the slice case
    protected final long fileLength;
    protected final long pageSizePower;
    protected final long pageSizeMask;
    protected final long sliceLength;
    protected final String basePathStr;
    // Current page state
    protected int currentPageIndex = -1;
    protected Page currentPage;
    protected long curOffsetInPage; // relative to curPage, not globally

    // Constructor for non-sliced files
    public IOInterceptingIndexInput( IndexInput underlyingIndexInput,
        String resourceDescription,
         IOContext context,
                                        BufferCache cache,
                                        String name,
                                         Path basePath) throws IOException {
        super(resourceDescription);
        this.delegate = underlyingIndexInput;
        this.context = context;
        this.off = 0;
        this.bufferCache = cache;
        this.name = name;
        this.isClone = false;
        this.pageSizePower = resolvePageSizeFromContext(context.readAdvice());
        this.pageSizeMask = (1L << pageSizePower) - 1L;
        this.currentPage = null;
        this.currentPageIndex = 0;
        this.curOffsetInPage = 0;
        this.fileLength = underlyingIndexInput.length();
        this.sliceLength = fileLength;//relative length
        this.basePathStr = basePath.toString();
        logger.debug("Creating IOInterceptingIndexInput for {} {} ", basePathStr, name);
    }

    // Constructor for sliced files
    public IOInterceptingIndexInput(String resourceDesc,
                                        IndexInput underlyingIndexInput,
                                        long off,
                                        long length,
                                        IOContext context,
                                        BufferCache cache,
                                        String name,
                                        long fileLength,
                                        String basePathStr) throws IOException {
        super(resourceDesc);
        this.delegate = underlyingIndexInput;
        this.off = off;
        this.isClone = true;
        this.context = context;
        this.bufferCache = cache;
        this.name = name;
        this.pageSizePower = resolvePageSizeFromContext(context.readAdvice());
        this.pageSizeMask = (1L << pageSizePower) - 1L;
        this.currentPageIndex = getPageIndex(off);
        this.curOffsetInPage = getPageOffset(off);
        this.fileLength = fileLength;
        this.sliceLength = length;
        this.basePathStr = basePathStr;
    }



    private long resolvePageSizeFromContext(ReadAdvice readAdvice) {
        return 12;
    }

    private long getPageOffset(long pos) {
        return pos & pageSizeMask;
    }

    private int getPageIndex(long pos) {
        return (int) (pos >> pageSizePower);
    }

    void ensureCurrentPageLoaded() throws IOException {
        currentPage = ensurePageIsLoaded(currentPageIndex);
    }

    private Page ensurePageIsLoaded(int pageIndex) throws IOException {
        return loadPage(pageIndex);
    }

    //must always be called by sequential reads
    void moveToNextPage() throws IOException {
        // Calculate the next page index
        int nextPageIndex = currentPageIndex + 1;
        // Check if next page would be beyond file bounds
        long nextPageStartOffset = (long) nextPageIndex << pageSizePower;
        if (nextPageStartOffset >= fileLength) {
            throw new EOFException("Cannot move to page " + nextPageIndex +
                " as it exceeds file bounds. File length: " + fileLength +
                ", page start offset: " + nextPageStartOffset + " current page index: " + currentPageIndex
                + " current offset in page: " + curOffsetInPage + " slice length: " + sliceLength
                + " current page size " + currentPage.getSize());
        }
        currentPageIndex = nextPageIndex;
        curOffsetInPage = 0;
        ensureCurrentPageLoaded();
    }

    long getAbsoluteCurrentPosition() {
        return getFilePointer() + off;
    }

    public void prefetch(long offset, long len) throws IOException {

           long actualOffset = offset + off; // Adjust for slice offset
           long startPageIndex = getPageIndex(actualOffset);
           long endOffsetRelative = offset + len;
           long endOffset = endOffsetRelative + off; // Last byte to prefetch
           long endPageIndex = getPageIndex(endOffset);
                    // Validate bounds
           if (actualOffset >= fileLength || actualOffset < 0) {
              logger.debug("Prefetch offset {} out of bounds for file length {}", actualOffset, fileLength);
              return;
           }

           // Adjust end offset if it exceeds file bounds
           long actualLength = Math.min(len, fileLength - actualOffset);
           if (actualLength <= 0) {
              return;
           }

                    // Recalculate end page with adjusted length
           endOffset = actualOffset + actualLength - 1;
           endPageIndex = getPageIndex(endOffset);

           logger.debug("Prefetching pages {} to {} for offset {} length {} (actual offset: {}, actual length: {})",
                        startPageIndex, endPageIndex, offset, len, actualOffset, actualLength);

                    // Prefetch all pages in the range
           for (int pageIndex = (int) startPageIndex; pageIndex <= endPageIndex; pageIndex++) {
               // Check if page is already in cache
               final int pageToPrefetch = pageIndex;
               String cacheKey = cacheKey(name, pageToPrefetch);
               if (bufferCache.get(cacheKey) != null) {
                   logger.debug("Page {} already in cache, skipping prefetch", pageToPrefetch);
               } else {
                   try {
                       validatePageIndex(pageToPrefetch);
                       // Load page into cache
                       Page page = bufferCache.getPageForReadAhead(cacheKey, (k, v) -> {
                           if (v == null) {
                               return createNewPage(pageToPrefetch);
                           }
                           return v;
                       });

                   } catch (IOException e) {
                       logger.warn("Failed to prefetch page {} for file {}: {}", pageToPrefetch, name, e.getMessage());
                   }
               }
           }
    }

    @Override
    public byte readByte() throws IOException {
        ensureCurrentPageLoaded();
        try {
            currentPage.getByte(curOffsetInPage);
            curOffsetInPage++;
        } catch (IndexOutOfBoundsException e) {
            long curPos = getAbsoluteCurrentPosition();
            if (curPos + 1 >= fileLength) {
                throw new EOFException("Attempted to read beyond end of file");
            }
            // Move to next page only if there's more data
            moveToNextPage();
            currentPage.getByte(curOffsetInPage);
            curOffsetInPage++;
        }
        return delegate.readByte();
    }

    @Override
    public short readShort() throws IOException {
        byte b1 = readByte();
        byte b2 = readByte();
        return (short)((b2 & 0xFF) << 8 | (b1 & 0xFF));
    }

    @Override
    public int readInt() throws IOException {
        byte b1 = readByte();
        byte b2 = readByte();
        byte b3 = readByte();
        byte b4 = readByte();
        return (b4 & 0xFF) << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }

    @Override
    public long readLong() throws IOException {
        byte b1 = readByte();
        byte b2 = readByte();
        byte b3 = readByte();
        byte b4 = readByte();
        byte b5 = readByte();
        byte b6 = readByte();
        byte b7 = readByte();
        byte b8 = readByte();
        return (b8 & 0xFFL) << 56
            | (b7 & 0xFFL) << 48
            | (b6 & 0xFFL) << 40
            | (b5 & 0xFFL) << 32
            | (b4 & 0xFFL) << 24
            | (b3 & 0xFFL) << 16
            | (b2 & 0xFFL) << 8
            | (b1 & 0xFFL);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        // Validate position
        RandomAccessInput randomAccessInput = (RandomAccessInput) delegate;
        byte b = randomAccessInput.readByte(pos);
        pos = pos + off; // Adjust for slice offset
        int pageIndex = getPageIndex(pos);
        long pageOffset = getPageOffset(pos);
        Page page = ensurePageIsLoaded(pageIndex);
        return b;
    }

    @Override
    public short readShort(long pos) throws IOException {

        byte b1 = readByte(pos - off);
        byte b2 = readByte(pos - off + 1);
        return (short)((b2 & 0xFF) << 8 | (b1 & 0xFF));

    }

    @Override
    public int readInt(long pos) throws IOException {
        pos = pos + off;
        // Int spans across pages - read byte by byte - it doesnt matter to do even read byte by byte as this
        //is only intended for intercepting page io
        byte b1 = readByte(pos - off);
        byte b2 = readByte(pos - off + 1);
        byte b3 = readByte(pos - off + 2);
        byte b4 = readByte(pos - off + 3);
        return (b4 & 0xFF) << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);

    }

    @Override
    public long readLong(long pos) throws IOException {

            byte b1 = readByte(pos - off);
            byte b2 = readByte(pos - off + 1);
            byte b3 = readByte(pos - off + 2);
            byte b4 = readByte(pos - off + 3);
            byte b5 = readByte(pos - off + 4);
            byte b6 = readByte(pos - off + 5);
            byte b7 = readByte(pos - off + 6);
            byte b8 = readByte(pos - off + 7);
            return (b8 & 0xFFL) << 56
                | (b7 & 0xFFL) << 48
                | (b6 & 0xFFL) << 40
                | (b5 & 0xFFL) << 32
                | (b4 & 0xFFL) << 24
                | (b3 & 0xFFL) << 16
                | (b2 & 0xFFL) << 8
                | (b1 & 0xFFL);


    }

    @Override
    public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
        RandomAccessInput.super.readBytes(pos, bytes, offset, length);
    }

    //sequential read pattern
    @Override
    public void readBytes(byte[] bytes, int off, int len) throws IOException {
        delegate.readBytes(bytes, off, len);//no validations needed post this
        int bytesRemaining = len;
        int destOffset = off;

        while (bytesRemaining > 0) {
            ensureCurrentPageLoaded();
            // Calculate how many bytes we can read from current page
            long availableInPage = currentPage.getSize() - curOffsetInPage;
            int bytesToRead = (int) Math.min(bytesRemaining, availableInPage);
            curOffsetInPage += bytesToRead;
            destOffset += bytesToRead;
            bytesRemaining -= bytesToRead;

            if (bytesRemaining > 0 && curOffsetInPage >= currentPage.getSize()) {
                // Check if we're at the end of the file
                long currentFilePosition = getAbsoluteCurrentPosition();
                //Only move to next page if there's more data in the file
                long nextPageStartOffset = ((long) (currentPageIndex + 1)) << pageSizePower;
                if (nextPageStartOffset < fileLength) {
                    moveToNextPage();
                } else {
                    //Should never land here
                    // We're at the end of the file, no more pages available
                    throw new EOFException("No more data available. Requested " + bytesRemaining +
                        " more bytes but reached end of file at position " + currentFilePosition);
                }
            }
        }
    }


    Page loadPage(int pageIndex) throws IOException {
        validatePageIndex(pageIndex);
        Page page = bufferCache.getPage(cacheKey(name, pageIndex), (k, v) -> {
            if (v == null) {
                return createNewPage(pageIndex);
            }
            return v;
        });
        return page;
    }

    //First time a new buffer is coming to life -> this is where we will be doing IO of 4kb.
    private Page createNewPage(int pageIndex) {
        final int pageSize = 1 << pageSizePower;//4096
        final long offsetInFile = (long) pageIndex << pageSizePower;
        int bytesToRead = (int) Math.min(pageSize, fileLength - offsetInFile);
        logger.info("IO is scheduled for file {} pageId {} from Thread {} ", basePathStr+name, pageIndex, Thread.currentThread().getName());
        return new Page(bytesToRead);

    }

    private void validatePageIndex(int pageIndex) throws IOException {
        if (pageIndex < 0) {
            throw new IllegalArgumentException("Page index cannot be negative: " + pageIndex);
        }

        final long offsetInFile = (long) pageIndex << pageSizePower;
        //
        if (offsetInFile >= fileLength) {
            EOFException eofException = new EOFException("Page index " + pageIndex + " exceeds file bounds");
            eofException.printStackTrace();
            logger.error("Error loading page {} for file {}", pageIndex, name, eofException);
            throw eofException;
        }
    }

    private String cacheKey(String name, int pageNumber) {
        return basePathStr + ":" +name + ":" + pageNumber;
    }

    @Override
    public void close() throws IOException {
        logger.debug("Closing IOInterceptingIndexInput for file: {}", name);
        // Clear held pages - they will be automatically cleaned up by Cleaner
        currentPage = null;
        delegate.close();
    }

    @Override
    public long getFilePointer() {
        long filePointer = delegate.getFilePointer();
        if (currentPageIndex == -1) return 0;
        long shadowFilePointer = ((long) currentPageIndex << pageSizePower) + curOffsetInPage - off;
        assert shadowFilePointer == filePointer;
        return filePointer;
    }

    @Override
    public void seek(long pos) throws IOException {
        delegate.seek(pos);
        pos = pos + off;
        int seekPageIndex = getPageIndex(pos);
        long seekOffsetInPage = getPageOffset(pos);

        if (seekPageIndex == currentPageIndex) {
            curOffsetInPage = seekOffsetInPage;
        } else {
            currentPageIndex = seekPageIndex;
            curOffsetInPage = seekOffsetInPage;
            currentPage = null; // load lazily
        }
    }

    @Override
    public long length() {
        return sliceLength;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((length | offset) < 0 || length > this.length() - offset) {
            throw new IllegalArgumentException(
                "slice() " + sliceDescription + " out of bounds: offset=" + offset +
                    ",length=" + length + ",fileLength=" + fileLength + ": " + this);
        }

        return new IOInterceptingIndexInput(
            getFullSliceDescription(sliceDescription),
            delegate.slice(sliceDescription, offset, length),
            off + offset,
            length,
            context,
            bufferCache,
            name,
            fileLength,
            basePathStr
        );
    }

    @Override
    public IndexInput clone() {
        try {
            return new IOInterceptingIndexInput(
                toString(),
                delegate.clone(),
                off,
                sliceLength,
                context,
                bufferCache,
                name,
                fileLength,
                basePathStr);
        } catch (IOException e) {
            throw new RuntimeException("Failed to clone IndexInput", e);
        }
    }
}
