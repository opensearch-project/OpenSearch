/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.catalog.s3;
// Constants holder: inlines Setting constants from repository-s3 S3Repository.
// Tech debt: extract to shared library so both plugins share the same definitions.

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;

import java.util.function.Function;

/**
 * Holder of S3 {@link Setting} definitions used by the copied S3 client
 * code (S3BlobStore, S3BlobContainer, S3ClientSettings, S3AsyncService, etc.).
 * The definitions mirror those in {@code org.opensearch.repositories.s3.S3Repository}
 * from the repository-s3 plugin. We inline them here because OpenSearch plugins
 * cannot depend on other plugins.
 */
public final class S3Settings {

    private S3Settings() {}

    // ---- Bucket and path settings ----

    public static final Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");

    public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    // ---- Buffer / multipart size constants ----

    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(
        Math.max(
            ByteSizeUnit.MB.toBytes(5),
            Math.min(ByteSizeUnit.MB.toBytes(100), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)
        ),
        ByteSizeUnit.BYTES
    );

    public static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "buffer_size",
        DEFAULT_BUFFER_SIZE,
        new ByteSizeValue(5, ByteSizeUnit.MB),
        new ByteSizeValue(5, ByteSizeUnit.GB)
    );

    public static final ByteSizeValue MAX_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    public static final ByteSizeValue MIN_PART_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.MB);

    public static final ByteSizeValue MAX_PART_SIZE_USING_MULTIPART = MAX_FILE_SIZE;

    public static final ByteSizeValue MAX_FILE_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.TB);

    // ---- Upload behavior settings ----

    public static final Setting<Boolean> REDIRECT_LARGE_S3_UPLOAD = Setting.boolSetting(
        "redirect_large_s3_upload",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> PERMIT_BACKED_TRANSFER_ENABLED = Setting.boolSetting(
        "permit_backed_transfer_enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> UPLOAD_RETRY_ENABLED = Setting.boolSetting(
        "s3_upload_retry_enabled",
        true,
        Setting.Property.NodeScope
    );

    private static final ByteSizeValue DEFAULT_MULTIPART_UPLOAD_MINIMUM_PART_SIZE = new ByteSizeValue(
        ByteSizeUnit.MB.toBytes(16),
        ByteSizeUnit.BYTES
    );

    public static final Setting<ByteSizeValue> PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING = Setting.byteSizeSetting(
        "parallel_multipart_upload.minimum_part_size",
        DEFAULT_MULTIPART_UPLOAD_MINIMUM_PART_SIZE,
        new ByteSizeValue(5, ByteSizeUnit.MB),
        new ByteSizeValue(5, ByteSizeUnit.GB),
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> PARALLEL_MULTIPART_UPLOAD_ENABLED_SETTING = Setting.boolSetting(
        "parallel_multipart_upload.enabled",
        true,
        Setting.Property.NodeScope
    );

    // ---- Bulk delete ----

    public static final Setting<Integer> BULK_DELETE_SIZE = Setting.intSetting("bulk_delete_size", 1000, 1, 1000);

    // ---- Storage class and ACL ----

    public static final Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");

    public static final Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl");

    // ---- Client name ----

    public static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    // ---- Credentials (insecure, stored in cluster state) ----

    public static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.insecureString("access_key");

    public static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.insecureString("secret_key");

    // ---- Server-side encryption ----

    public static final String BUCKET_DEFAULT_ENCRYPTION_TYPE = "bucket_default";

    public static final Setting<String> SERVER_SIDE_ENCRYPTION_TYPE_SETTING = Setting.simpleString(
        "server_side_encryption_type",
        BUCKET_DEFAULT_ENCRYPTION_TYPE
    );

    public static final Setting<String> SERVER_SIDE_ENCRYPTION_KMS_KEY_SETTING = Setting.simpleString(
        "server_side_encryption_kms_key_id"
    );

    public static final Setting<Boolean> SERVER_SIDE_ENCRYPTION_BUCKET_KEY_SETTING = Setting.boolSetting(
        "server_side_encryption_bucket_key_enabled",
        true
    );

    public static final Setting<String> SERVER_SIDE_ENCRYPTION_ENCRYPTION_CONTEXT_SETTING = Setting.simpleString(
        "server_side_encryption_encryption_context"
    );

    // ---- Expected bucket owner ----

    public static final Setting<String> EXPECTED_BUCKET_OWNER_SETTING = Setting.simpleString("expected_bucket_owner");

    // ---- Async HTTP client type ----

    public static final String NETTY_ASYNC_HTTP_CLIENT_TYPE = "netty";
    public static final String CRT_ASYNC_HTTP_CLIENT_TYPE = "crt";

    public static final Setting<String> S3_ASYNC_HTTP_CLIENT_TYPE = Setting.simpleString(
        "s3_async_client_type",
        CRT_ASYNC_HTTP_CLIENT_TYPE,
        Setting.Property.NodeScope
    );
}
