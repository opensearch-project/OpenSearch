/* * SPDX-License-Identifier: Apache-2.0 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.Randomness;
import org.opensearch.common.util.io.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.FilterOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;

import java.util.function.Supplier;

/**
 * A hybrid directory implementation that encrypts files
 * to be stored based on a user supplied key
 *
 * @opensearch.internal
 */
public final class CryptoDirectory extends NIOFSDirectory {
    private final MMapDirectory delegate;
    private final Set<String> mmapExtensions;
    private Key dataKey;
    private ConcurrentSkipListMap<String, String> ivMap;
    private final Provider provider;

    private final AtomicLong nextTempFileCounter = new AtomicLong();

    CryptoDirectory(
        LockFactory lockFactory,
        MMapDirectory delegate,
        Set<String> mmapExtensions,
        Provider provider,
        String alias,
        Supplier<String> passwordSupplier,
        String keyStorePath
    ) throws IOException {
        super(delegate.getDirectory(), lockFactory);
        this.delegate = delegate;
        this.mmapExtensions = mmapExtensions;
        ivMap = new ConcurrentSkipListMap<>();
        IndexInput in;
        this.provider = provider;
        try {
            in = super.openInput("ivMap", new IOContext());
        } catch (java.nio.file.NoSuchFileException nsfe) {
            in = null;
        }
        LocalKeyStoreManager keyStore = LocalKeyStoreManager.load(keyStorePath, alias, passwordSupplier);
        if (in != null) {
            Map<String, String> tmp = in.readMapOfStrings();
            ivMap.putAll(tmp);
            in.close();
            dataKey = keyStore.decryptDataKey(getWrappedKey());
        } else {
            dataKey = keyStore.generateDataKey();
            storeWrappedKey(keyStore.wrapDataKey(dataKey));
        }
    }

    private void storeWrappedKey(byte[] wrappedKey) {
        try {
            IndexOutput out = super.createOutput("keyfile", new IOContext());
            out.writeInt(wrappedKey.length);
            out.writeBytes(wrappedKey, 0, wrappedKey.length);
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] getWrappedKey() {
        try {
            IndexInput in = super.openInput("keyfile", new IOContext());
            int size = in.readInt();
            byte[] ret = new byte[size];
            in.readBytes(ret, 0, size);
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        if (!(source.contains("segments_") || source.endsWith(".si"))) ivMap.put(
            getDirectory() + "/" + dest,
            ivMap.remove(getDirectory() + "/" + source)
        );
        super.rename(source, dest);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (name.contains("segments_") || name.endsWith(".si")) return super.openInput(name, context);
        ensureOpen();
        ensureCanRead(name);
        Path path = getDirectory().resolve(name);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
        boolean success = false;
        try {
            Cipher cipher = CipherFactory.getCipher(provider);
            String ivEntry = ivMap.get(getDirectory() + "/" + name);
            Objects.requireNonNull(ivEntry, "Failed to open file");
            byte[] iv = Base64.getDecoder().decode(ivEntry);
            CipherFactory.initCipher(cipher, this, Optional.of(iv), Cipher.DECRYPT_MODE, 0);
            final IndexInput indexInput;
            if (useDelegate(name)) {
                indexInput = new CryptoMMapIndexInput(
                    delegate.openInput(name, context),
                    "CryptoMMapIndexInput(path=\"" + getDirectory().resolve(name) + "\")",
                    cipher,
                    this
                );
                success = true;
            } else {
                indexInput = new DecryptingFSIndexInput("DecryptingFSIndexInput(path=\"" + path + "\")", fc, context, cipher, this);
                success = true;
            }
            return indexInput;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(fc);
            }
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        if (name.contains("segments_") || name.endsWith(".si")) return super.createOutput(name, context);
        ensureOpen();
        // maybeDeletePendingFiles();
        // If this file was pending delete, we are now bringing it back to life:
        // if (pendingDeletes.remove(name)) {
        // privateDeleteFile(name, true); // try again to delete it - this is best effort
        // pendingDeletes.remove(name); // watch out - if the delete fails it put
        // }
        OutputStream fos = Files.newOutputStream(directory.resolve(name), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        Cipher cipher = CipherFactory.getCipher(provider);
        SecureRandom random = Randomness.createSecure();
        byte[] iv = new byte[16];
        random.nextBytes(iv);
        if (dataKey == null) throw new RuntimeException("dataKey is null!");
        CipherFactory.initCipher(cipher, this, Optional.of(iv), Cipher.ENCRYPT_MODE, 0);
        ivMap.put(getDirectory() + "/" + name, Base64.getEncoder().encodeToString(iv));
        return new EncryptingFSIndexOutput(name, fos, cipher);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        if (prefix.contains("segments_") || prefix.endsWith(".si")) return super.createTempOutput(prefix, suffix, context);
        ensureOpen();
        // maybeDeletePendingFiles();
        String name;
        while (true) {
            // try {
            name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
            // if (pendingDeletes.contains(name)) {
            // continue;
            // }
            // } catch (
            // @SuppressWarnings("unused")
            // FileAlreadyExistsException faee) {
            // Retry with next incremented name
            // }
            OutputStream fos = Files.newOutputStream(directory.resolve(name), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

            Cipher cipher = CipherFactory.getCipher(provider);
            SecureRandom random = Randomness.createSecure();
            byte[] iv = new byte[16];
            random.nextBytes(iv);
            CipherFactory.initCipher(cipher, this, Optional.of(iv), Cipher.ENCRYPT_MODE, 0);
            ivMap.put(getDirectory() + "/" + name, Base64.getEncoder().encodeToString(iv));
            return new EncryptingFSIndexOutput(name, fos, cipher);
        }
    }

    boolean useDelegate(String name) {
        final String extension = FileSwitchDirectory.getExtension(name);
        return mmapExtensions.contains(extension);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            deleteFile("ivMap");
        } catch (java.nio.file.NoSuchFileException fnfe) {

        }
        IndexOutput out = super.createOutput("ivMap", new IOContext());
        out.writeMapOfStrings(ivMap);
        out.close();
        isOpen = false;
        deletePendingFiles();
        dataKey = null;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ivMap.remove(getDirectory() + "/" + name);
        super.deleteFile(name);
    }

    MMapDirectory getDelegate() {
        return delegate;
    }

    /**
     * An IndexInput implementation that wraps an existing
     * ByteBufferIndexInput and decrypts data for reading
     *
     * @opensearch.internal
     */
    final class CryptoMMapIndexInput extends IndexInput {
        private IndexInput in;
        private Cipher cipher;
        private CryptoDirectory directory;
        long offset;
        long length;
        byte[] tmp;

        public CryptoMMapIndexInput(IndexInput in, String description, Cipher cipher, CryptoDirectory directory) {
            super(description);
            this.in = in;
            this.cipher = cipher;
            this.directory = directory;
            offset = 0;
            tmp = new byte[8];
        }

        public CryptoMMapIndexInput(IndexInput in, String description, Cipher old, CryptoDirectory directory, long offset, long length) {
            super(description);
            this.in = in;
            this.directory = directory;
            this.offset = offset;
            this.length = length;
            tmp = new byte[8];
            cipher = CipherFactory.getCipher(provider);
            CipherFactory.initCipher(cipher, directory, Optional.of(old.getIV()), Cipher.DECRYPT_MODE, offset);
        }

        @Override
        public IndexInput clone() {
            return new CryptoMMapIndexInput(in.clone(), getFullSliceDescription(""), cipher, directory, this.offset, length());
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos > length()) {
                throw new EOFException(
                    Thread.currentThread().getId() + " read past EOF: pos=" + pos + " vs length=" + length() + ": " + this
                );
            }
            long current = getFilePointer();
            if (current <= pos && (pos + offset) / 16 == (current + offset) / 16) {
                cipher.update(new byte[(int) (pos - current)]);
                in.seek(pos);
                return;
            }
            in.seek(pos);
            CipherFactory.initCipher(cipher, directory, Optional.empty(), Cipher.DECRYPT_MODE, pos + offset);
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public byte readByte() throws IOException {
            return cipher.update(new byte[] { in.readByte() })[0];
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            byte[] tmp = new byte[len];
            in.readBytes(tmp, 0, len);
            try {
                cipher.update(tmp, 0, len, b, offset);
            } catch (ShortBufferException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
            byte[] tmp = new byte[len];
            in.readBytes(tmp, 0, len, useBuffer);
            try {
                cipher.update(tmp, 0, len, b, offset);
            } catch (ShortBufferException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int readInt() throws IOException {
            readBytes(tmp, 0, 4);
            return ((tmp[3] & 0xFF) << 24) | ((tmp[2] & 0xFF) << 16) | ((tmp[1] & 0xFF) << 8) | (tmp[0] & 0xFF);
        }

        @Override
        public long readLong() throws IOException {
            readBytes(tmp, 0, 8);
            return ((tmp[7] & 0xFFL) << 56) | ((tmp[6] & 0xFFL) << 48) | ((tmp[5] & 0xFFL) << 40) | ((tmp[4] & 0xFFL) << 32) | ((tmp[3]
                & 0xFFL) << 24) | ((tmp[2] & 0xFFL) << 16) | ((tmp[1] & 0xFFL) << 8) | (tmp[0] & 0xFFL);
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {
            seek(getFilePointer() + numBytes);
        }

        @Override
        public CryptoMMapIndexInput slice(String sliceDescription, long offset, long newLength) throws IOException {
            if (offset < 0 || newLength < 0 || offset + newLength > this.length()) throw new RuntimeException(
                "bad slice! current offset: " + this.offset + " length: " + length() + " newOffset: " + offset + " newLength: " + newLength
            );
            return new CryptoMMapIndexInput(
                in.slice(sliceDescription, offset, newLength),
                getFullSliceDescription(sliceDescription),
                cipher,
                directory,
                this.offset + offset,
                newLength
            );
        }
    }

    /**
     * An IndexInput implementation that decrypts data for reading
     *
     * @opensearch.internal
     */
    final class DecryptingFSIndexInput extends BufferedIndexInput {
        /** The maximum chunk size for reads of 16384 bytes. */
        private static final int CHUNK_SIZE = 16384;
        ByteBuffer tmpBuffer = ByteBuffer.allocate(CHUNK_SIZE);

        /** the file channel we will read from */
        protected final FileChannel channel;
        /** is this instance a clone and hence does not own the file to close it */
        boolean isClone = false;
        /** start offset: non-zero in the slice case */
        protected final long off;
        /** end offset (start+length) */
        protected final long end;
        InputStream stream;
        Cipher cipher;
        CryptoDirectory directory;

        public DecryptingFSIndexInput(String resourceDesc, FileChannel fc, IOContext context, Cipher cipher, CryptoDirectory directory)
            throws IOException {
            super(resourceDesc, context);
            this.channel = fc;
            this.off = 0L;
            this.end = fc.size();
            this.stream = Channels.newInputStream(channel);
            this.cipher = cipher;
            this.directory = directory;
        }

        public DecryptingFSIndexInput(
            String resourceDesc,
            FileChannel fc,
            long off,
            long length,
            int bufferSize,
            Cipher old,
            CryptoDirectory directory,
            InputStream stream
        ) throws IOException {
            super(resourceDesc, bufferSize);
            this.channel = fc;
            this.off = off;
            this.end = off + length;
            this.isClone = true;
            this.directory = directory;
            this.stream = stream;
            cipher = CipherFactory.getCipher(old.getProvider());
            CipherFactory.initCipher(cipher, directory, Optional.of(old.getIV()), Cipher.DECRYPT_MODE, off);
        }

        @Override
        public void close() throws IOException {
            if (!isClone) {
                stream.close();
            }
        }

        @Override
        public DecryptingFSIndexInput clone() {
            try {
                DecryptingFSIndexInput clone = new DecryptingFSIndexInput(
                    getFullSliceDescription(""),
                    channel,
                    off,
                    length(),
                    getBufferSize(),
                    cipher,
                    directory,
                    stream
                );
                clone.isClone = true;
                return clone;
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException(
                    "slice() "
                        + sliceDescription
                        + " out of bounds: offset="
                        + offset
                        + ",length="
                        + length
                        + ",fileLength="
                        + this.length()
                        + ": "
                        + this
                );
            }
            return new DecryptingFSIndexInput(
                getFullSliceDescription(sliceDescription),
                channel,
                off + offset,
                length,
                getBufferSize(),
                cipher,
                directory,
                stream
            );
        }

        @Override
        public final long length() {
            return end - off;
        }

        private int read(ByteBuffer dst, long position) throws IOException {
            int ret;
            int i;
            tmpBuffer.rewind();
            channel.position(position);
            i = stream.read(tmpBuffer.array(), 0, dst.remaining());
            tmpBuffer.limit(i);
            try {
                if (end - position > i) ret = cipher.update(tmpBuffer, dst);
                else ret = cipher.doFinal(tmpBuffer, dst);
            } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException ex) {
                throw new IOException("failed to decrypt blck.", ex);
            }
            return ret;
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            long pos = getFilePointer() + off;

            if (pos + b.remaining() > end) {
                throw new EOFException(
                    Thread.currentThread().getId()
                        + " read past EOF: "
                        + this
                        + " isClone? "
                        + isClone
                        + " off: "
                        + off
                        + " pos: "
                        + pos
                        + " end: "
                        + end
                );
            }

            try {
                int readLength = b.remaining();
                while (readLength > 0) {
                    final int toRead = Math.min(CHUNK_SIZE, readLength);
                    b.limit(b.position() + toRead);
                    assert b.remaining() == toRead;
                    final int i = read(b, pos);
                    if (i < 0) {
                        throw new EOFException("read past EOF: " + this + " buffer: " + b + " chunkLen: " + toRead + " end: " + end);
                    }
                    assert i > 0 : "FileChannel.read with non zero-length bb.remaining() must always read at least "
                        + "one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";
                    pos += i;
                    readLength -= i;
                }
                assert readLength == 0;
            } catch (IOException ioe) {
                throw new IOException(ioe.getMessage() + ": " + this, ioe);
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length()) {
                throw new EOFException(
                    Thread.currentThread().getId() + " read past EOF: pos=" + pos + " vs length=" + length() + ": " + this
                );
            }
            CipherFactory.initCipher(cipher, directory, Optional.empty(), Cipher.DECRYPT_MODE, pos + off);
        }
    }

    /**
     * An IndexOutput implementation that encrypts data before writing
     *
     * @opensearch.internal
     */
    final class EncryptingFSIndexOutput extends OutputStreamIndexOutput {
        /**
         * The maximum chunk size is 8192 bytes, because file channel mallocs a native buffer outside of
         * stack if the write buffer size is larger.
         */
        static final int CHUNK_SIZE = 8192;
        Cipher cipher;

        public EncryptingFSIndexOutput(String name, OutputStream os, Cipher cipher) throws IOException {
            super("FSIndexOutput(path=\"" + directory.resolve(name) + "\")", name, new FilterOutputStream(os) {

                @Override
                public void close() throws IOException {
                    try {
                        out.write(cipher.doFinal());
                    } catch (IllegalBlockSizeException | BadPaddingException e) {
                        throw new RuntimeException(e);
                    }
                    super.close();
                }

                @Override
                public void write(byte[] b, int offset, int length) throws IOException {
                    int count = 0;
                    byte[] res;
                    while (length > 0) {
                        count++;
                        final int chunk = Math.min(length, CHUNK_SIZE);
                        try {
                            res = cipher.update(b, offset, chunk);
                            if (res != null) out.write(res);
                        } catch (IllegalStateException e) {
                            throw new IllegalStateException("count is " + count + " " + e.getMessage());
                        }
                        length -= chunk;
                        offset += chunk;
                    }
                }
            }, CHUNK_SIZE);
            this.cipher = cipher;
        }
    }

    static class CipherFactory {
        static final int AES_BLOCK_SIZE = 16;

        public static Cipher getCipher(Provider provider) {
            try {
                return Cipher.getInstance("AES/CTR/NoPadding", provider);
            } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
                throw new RuntimeException();
            }
        }

        public static void initCipher(Cipher cipher, CryptoDirectory directory, Optional<byte[]> ivarray, int opmode, long newPosition) {
            try {
                byte[] iv = ivarray.isPresent() ? ivarray.get() : cipher.getIV();
                if (newPosition == 0) {
                    Arrays.fill(iv, 12, 16, (byte) 0);
                } else {
                    int counter = (int) (newPosition / AES_BLOCK_SIZE);
                    for (int i = 15; i >= 12; i--) {
                        iv[i] = (byte) counter;
                        counter = counter >>> Byte.SIZE;
                    }
                }
                IvParameterSpec spec = new IvParameterSpec(iv);
                cipher.init(opmode, directory.dataKey, spec);
                int bytesToRead = (int) (newPosition % AES_BLOCK_SIZE);
                if (bytesToRead > 0) {
                    cipher.update(new byte[bytesToRead]);
                }
            } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
