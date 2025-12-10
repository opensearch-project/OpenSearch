/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Optional;

/**
 * CryptoMetadata for AWS KMS encryption.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class KmsCryptoMetadata extends CryptoMetadata {

    private static final String KMS_KEY_ARN_SETTING = "kms.key_arn";
    private static final String KMS_ENCRYPTION_CONTEXT_SETTING = "kms.encryption_context";
    private final boolean isIndexLevel;

    public KmsCryptoMetadata(String keyProviderName, Settings settings, boolean isIndexLevel) {
        super(keyProviderName, "aws-kms", settings);
        this.isIndexLevel = isIndexLevel;
    }

    public KmsCryptoMetadata(StreamInput in) throws IOException {
        super(in);
        this.isIndexLevel = in.readBoolean();
    }

    @Override
    public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(isIndexLevel);
    }

    public boolean isIndexLevel() {
        return isIndexLevel;
    }

    public Optional<String> getKmsKeyArn() {
        return Optional.ofNullable(settings().get(KMS_KEY_ARN_SETTING));
    }

    public Optional<String> getKmsEncryptionContext() {
        return Optional.ofNullable(settings().get(KMS_ENCRYPTION_CONTEXT_SETTING));
    }

    public static KmsCryptoMetadata fromIndexSettings(Settings indexSettings) {
        String keyProviderName = indexSettings.get("index.store.crypto.key_provider");
        if (keyProviderName == null) {
            return null;
        }

        Settings cryptoSettings = indexSettings.getAsSettings("index.store.crypto");
        return new KmsCryptoMetadata(keyProviderName, cryptoSettings, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        KmsCryptoMetadata that = (KmsCryptoMetadata) o;
        return isIndexLevel == that.isIndexLevel;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (isIndexLevel ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "KmsCryptoMetadata{"
            + "keyProviderName="
            + keyProviderName()
            + ", kmsKeyArn="
            + getKmsKeyArn().orElse("none")
            + ", isIndexLevel="
            + isIndexLevel
            + "}";
    }
}
