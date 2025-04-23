/*
 * SPDX-License-Identifier: Apache-2.0
 *
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

package org.opensearch.tools.cli.keystore;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class KeyStoreWrapperTests extends OpenSearchTestCase {

    Environment env;
    List<FileSystem> fileSystems = new ArrayList<>();

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystems);
    }

    @Before
    public void setupEnv() throws IOException {
        env = KeyStoreCommandTestCase.setupEnv(true, fileSystems);
    }

    public void testFileSettingExhaustiveBytes() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; ++i) {
            bytes[i] = (byte) i;
        }
        keystore.setFile("foo", bytes);
        keystore.save(env.configDir(), new char[0]);
        keystore = KeyStoreWrapper.load(env.configDir());
        keystore.decrypt(new char[0]);
        try (InputStream stream = keystore.getFile("foo")) {
            for (int i = 0; i < 256; ++i) {
                int got = stream.read();
                if (got < 0) {
                    fail("Expected 256 bytes but read " + i);
                }
                assertEquals(i, got);
            }
            assertEquals(-1, stream.read()); // nothing left
        }
    }

    public void testCreate() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        assertTrue(keystore.getSettingNames().contains(KeyStoreWrapper.SEED_SETTING.getKey()));
    }

    public void testDecryptKeyStoreWithWrongPassword() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.save(env.configDir(), new char[0]);
        final KeyStoreWrapper loadedKeystore = KeyStoreWrapper.load(env.configDir());
        final SecurityException exception = expectThrows(
            SecurityException.class,
            () -> loadedKeystore.decrypt(new char[] { 'i', 'n', 'v', 'a', 'l', 'i', 'd' })
        );
        assertThat(
            exception.getMessage(),
            anyOf(
                containsString("Provided keystore password was incorrect"),
                containsString("Keystore has been corrupted or tampered with")
            )
        );
    }

    public void testCannotReadStringFromClosedKeystore() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        assertThat(keystore.getSettingNames(), Matchers.hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        assertThat(keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()), notNullValue());

        keystore.close();

        assertThat(keystore.getSettingNames(), Matchers.hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey())
        );
        assertThat(exception.getMessage(), containsString("closed"));
    }

    public void testValueSHA256Digest() throws Exception {
        final KeyStoreWrapper keystore = KeyStoreWrapper.create();
        final String stringSettingKeyName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + "1";
        final String stringSettingValue = randomAlphaOfLength(32);
        keystore.setString(stringSettingKeyName, stringSettingValue.toCharArray());
        final String fileSettingKeyName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + "2";
        final byte[] fileSettingValue = randomByteArrayOfLength(32);
        keystore.setFile(fileSettingKeyName, fileSettingValue);

        final byte[] stringSettingHash = MessageDigest.getInstance("SHA-256").digest(stringSettingValue.getBytes(StandardCharsets.UTF_8));
        assertThat(keystore.getSHA256Digest(stringSettingKeyName), equalTo(stringSettingHash));
        final byte[] fileSettingHash = MessageDigest.getInstance("SHA-256").digest(fileSettingValue);
        assertThat(keystore.getSHA256Digest(fileSettingKeyName), equalTo(fileSettingHash));

        keystore.close();

        // value hashes accessible even when the keystore is closed
        assertThat(keystore.getSHA256Digest(stringSettingKeyName), equalTo(stringSettingHash));
        assertThat(keystore.getSHA256Digest(fileSettingKeyName), equalTo(fileSettingHash));
    }

    public void testUpgradeNoop() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configDir(), new char[0]);
        // upgrade does not overwrite seed
        KeyStoreWrapper.upgrade(keystore, env.configDir(), new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
        keystore = KeyStoreWrapper.load(env.configDir());
        keystore.decrypt(new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }

    public void testFailWhenCannotConsumeSecretStream() throws Exception {
        Path configDir = env.configDir();
        NIOFSDirectory directory = new NIOFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("opensearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "opensearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);
            // Indicate that the secret string is longer than it is so readFully() fails
            possiblyAlterSecretString(output, -4);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, 0);
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
        assertThat(e.getCause(), instanceOf(EOFException.class));
    }

    public void testFailWhenCannotConsumeEncryptedBytesStream() throws Exception {
        Path configDir = env.configDir();
        NIOFSDirectory directory = new NIOFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("opensearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "opensearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);

            possiblyAlterSecretString(output, 0);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            // Indicate that the encryptedBytes is larger than it is so readFully() fails
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, -12);
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
        assertThat(e.getCause(), instanceOf(EOFException.class));
    }

    public void testFailWhenSecretStreamNotConsumed() throws Exception {
        Path configDir = env.configDir();
        NIOFSDirectory directory = new NIOFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("opensearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "opensearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);
            // So that readFully during decryption will not consume the entire stream
            possiblyAlterSecretString(output, 4);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, 0);
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
    }

    public void testFailWhenEncryptedBytesStreamIsNotConsumed() throws Exception {
        Path configDir = env.configDir();
        NIOFSDirectory directory = new NIOFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("opensearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "opensearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);
            possiblyAlterSecretString(output, 0);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, randomIntBetween(2, encryptedBytes.length));
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
    }

    private CipherOutputStream getCipherStream(ByteArrayOutputStream bytes, byte[] salt, byte[] iv) throws Exception {
        PBEKeySpec keySpec = new PBEKeySpec(new char[0], salt, 10000, 128);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        SecretKey secretKey = keyFactory.generateSecret(keySpec);
        SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), "AES");
        GCMParameterSpec spec = new GCMParameterSpec(128, iv);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, secret, spec);
        cipher.updateAAD(salt);
        return new CipherOutputStream(bytes, cipher);
    }

    private void possiblyAlterSecretString(DataOutputStream output, int truncLength) throws Exception {
        byte[] secret_value = "super_secret_value".getBytes(StandardCharsets.UTF_8);
        output.writeInt(1); // One entry
        output.writeUTF("string_setting");
        output.writeUTF("STRING");
        output.writeInt(secret_value.length - truncLength);
        output.write(secret_value);
    }

    private void possiblyAlterEncryptedBytes(
        IndexOutput indexOutput,
        byte[] salt,
        byte[] iv,
        byte[] encryptedBytes,
        int truncEncryptedDataLength
    ) throws Exception {
        DataOutput io = EndiannessReverserUtil.wrapDataOutput(indexOutput);
        io.writeInt(4 + salt.length + 4 + iv.length + 4 + encryptedBytes.length);
        io.writeInt(salt.length);
        io.writeBytes(salt, salt.length);
        io.writeInt(iv.length);
        io.writeBytes(iv, iv.length);
        io.writeInt(encryptedBytes.length - truncEncryptedDataLength);
        io.writeBytes(encryptedBytes, encryptedBytes.length);
    }

    public void testUpgradeAddsSeed() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.remove(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configDir(), new char[0]);
        KeyStoreWrapper.upgrade(keystore, env.configDir(), new char[0]);
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        assertNotNull(seed);
        keystore = KeyStoreWrapper.load(env.configDir());
        keystore.decrypt(new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }

    public void testIllegalSettingName() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> KeyStoreWrapper.validateSettingName("*"));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        e = expectThrows(IllegalArgumentException.class, () -> keystore.setString("*", new char[0]));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
        e = expectThrows(IllegalArgumentException.class, () -> keystore.setFile("*", new byte[0]));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
    }

    public void testBackcompatV1() throws Exception {
        assumeFalse("Can't run in a FIPS JVM as PBE is not available", inFipsJvm());
        generateV1();
        Path configDir = env.configDir();
        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        keystore.decrypt(new char[0]);
        SecureString testValue = keystore.getString("string_setting");
        assertThat(testValue.toString(), equalTo("stringSecretValue"));
    }

    public void testBackcompatV2() throws Exception {
        assumeFalse("Can't run in a FIPS JVM as PBE is not available", inFipsJvm());
        byte[] fileBytes = generateV2();
        Path configDir = env.configDir();
        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        keystore.decrypt(new char[0]);
        SecureString testValue = keystore.getString("string_setting");
        assertThat(testValue.toString(), equalTo("stringSecretValue"));

        try (InputStream fileInput = keystore.getFile("file_setting")) {
            byte[] readBytes = new byte[20];
            assertEquals(20, fileInput.read(readBytes));
            for (int i = 0; i < fileBytes.length; ++i) {
                assertThat("byte " + i, readBytes[i], equalTo(fileBytes[i]));
            }
            assertEquals(-1, fileInput.read());
        }
    }

    public void testStringAndFileDistinction() throws Exception {
        final KeyStoreWrapper wrapper = KeyStoreWrapper.create();
        wrapper.setString("string_setting", "string_value".toCharArray());
        final Path temp = createTempDir();
        Files.write(temp.resolve("file_setting"), "file_value".getBytes(StandardCharsets.UTF_8));
        wrapper.setFile("file_setting", Files.readAllBytes(temp.resolve("file_setting")));
        wrapper.save(env.configDir(), new char[0]);
        wrapper.close();

        final KeyStoreWrapper afterSave = KeyStoreWrapper.load(env.configDir());
        assertNotNull(afterSave);
        afterSave.decrypt(new char[0]);
        assertThat(afterSave.getSettingNames(), equalTo(new HashSet<>(Arrays.asList("keystore.seed", "string_setting", "file_setting"))));
        assertThat(afterSave.getString("string_setting"), equalTo("string_value"));
        assertThat(toByteArray(afterSave.getFile("string_setting")), equalTo("string_value".getBytes(StandardCharsets.UTF_8)));
        assertThat(afterSave.getString("file_setting"), equalTo("file_value"));
        assertThat(toByteArray(afterSave.getFile("file_setting")), equalTo("file_value".getBytes(StandardCharsets.UTF_8)));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/468")
    public void testLegacyV3() throws GeneralSecurityException, IOException {
        final Path configDir = createTempDir();
        final Path keystore = configDir.resolve("opensearch.keystore");
        try (
            InputStream is = KeyStoreWrapperTests.class.getResourceAsStream("/format-v3-opensearch.keystore");
            OutputStream os = Files.newOutputStream(keystore)
        ) {
            final byte[] buffer = new byte[4096];
            int readBytes;
            while ((readBytes = is.read(buffer)) > 0) {
                os.write(buffer, 0, readBytes);
            }
        }
        final KeyStoreWrapper wrapper = KeyStoreWrapper.load(configDir);
        assertNotNull(wrapper);
        wrapper.decrypt(new char[0]);
        assertThat(wrapper.getFormatVersion(), equalTo(3));
        assertThat(wrapper.getSettingNames(), equalTo(new HashSet<>(Arrays.asList("keystore.seed", "string_setting", "file_setting"))));
        assertThat(wrapper.getString("string_setting"), equalTo("string_value"));
        assertThat(toByteArray(wrapper.getFile("string_setting")), equalTo("string_value".getBytes(StandardCharsets.UTF_8)));
        assertThat(wrapper.getString("file_setting"), equalTo("file_value"));
        assertThat(toByteArray(wrapper.getFile("file_setting")), equalTo("file_value".getBytes(StandardCharsets.UTF_8)));
    }

    private void generateV1() throws IOException, NoSuchAlgorithmException, NoSuchProviderException, CertificateException,
        InvalidKeySpecException, KeyStoreException {
        Path configDir = env.configDir();
        NIOFSDirectory directory = new NIOFSDirectory(configDir);
        try (IndexOutput output = EndiannessReverserUtil.createOutput(directory, "opensearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, "opensearch.keystore", 1);
            output.writeByte((byte) 0); // hasPassword = false
            output.writeString("PKCS12");
            output.writeString("PBE");

            SecretKeyFactory secretFactory = SecretKeyFactory.getInstance("PBE", "SunJCE");
            KeyStore keystore = KeyStore.getInstance("PKCS12", "SUN");
            keystore.load(null, null);
            SecretKey secretKey = secretFactory.generateSecret(new PBEKeySpec("stringSecretValue".toCharArray()));
            KeyStore.ProtectionParameter protectionParameter = new KeyStore.PasswordProtection(new char[0]);
            keystore.setEntry("string_setting", new KeyStore.SecretKeyEntry(secretKey), protectionParameter);

            ByteArrayOutputStream keystoreBytesStream = new ByteArrayOutputStream();
            keystore.store(keystoreBytesStream, new char[0]);
            byte[] keystoreBytes = keystoreBytesStream.toByteArray();
            output.writeInt(keystoreBytes.length);
            output.writeBytes(keystoreBytes, keystoreBytes.length);
            CodecUtil.writeFooter(output);
        }
    }

    private byte[] generateV2() throws Exception {
        Path configDir = env.configDir();
        NIOFSDirectory directory = new NIOFSDirectory(configDir);
        byte[] fileBytes = new byte[20];
        random().nextBytes(fileBytes);
        try (IndexOutput output = EndiannessReverserUtil.createOutput(directory, "opensearch.keystore", IOContext.DEFAULT)) {

            CodecUtil.writeHeader(output, "opensearch.keystore", 2);
            output.writeByte((byte) 0); // hasPassword = false
            output.writeString("PKCS12");
            output.writeString("PBE"); // string algo
            output.writeString("PBE"); // file algo

            output.writeVInt(2); // num settings
            output.writeString("string_setting");
            output.writeString("STRING");
            output.writeString("file_setting");
            output.writeString("FILE");

            SecretKeyFactory secretFactory = SecretKeyFactory.getInstance("PBE", "SunJCE");
            KeyStore keystore = KeyStore.getInstance("PKCS12", "SUN");
            keystore.load(null, null);
            SecretKey secretKey = secretFactory.generateSecret(new PBEKeySpec("stringSecretValue".toCharArray()));
            KeyStore.ProtectionParameter protectionParameter = new KeyStore.PasswordProtection(new char[0]);
            keystore.setEntry("string_setting", new KeyStore.SecretKeyEntry(secretKey), protectionParameter);

            byte[] base64Bytes = Base64.getEncoder().encode(fileBytes);
            char[] chars = new char[base64Bytes.length];
            for (int i = 0; i < chars.length; ++i) {
                chars[i] = (char) base64Bytes[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
            }
            secretKey = secretFactory.generateSecret(new PBEKeySpec(chars));
            keystore.setEntry("file_setting", new KeyStore.SecretKeyEntry(secretKey), protectionParameter);

            ByteArrayOutputStream keystoreBytesStream = new ByteArrayOutputStream();
            keystore.store(keystoreBytesStream, new char[0]);
            byte[] keystoreBytes = keystoreBytesStream.toByteArray();
            output.writeInt(keystoreBytes.length);
            output.writeBytes(keystoreBytes, keystoreBytes.length);
            CodecUtil.writeFooter(output);
        }

        return fileBytes;
    }

    private byte[] toByteArray(final InputStream is) throws IOException {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final byte[] buffer = new byte[1024];
        int readBytes;
        while ((readBytes = is.read(buffer)) > 0) {
            os.write(buffer, 0, readBytes);
        }
        return os.toByteArray();
    }

}
