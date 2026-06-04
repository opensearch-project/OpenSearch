/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Processor that sets the _routing field based on ACL metadata.
 */
public final class AclRoutingProcessor extends AbstractProcessor {

    public static final String TYPE = "acl_routing";
    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final String aclField;
    private final String targetField;
    private final boolean ignoreMissing;
    private final boolean overrideExisting;

    AclRoutingProcessor(
        String tag,
        String description,
        String aclField,
        String targetField,
        boolean ignoreMissing,
        boolean overrideExisting
    ) {
        super(tag, description);
        this.aclField = aclField;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.overrideExisting = overrideExisting;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        Object aclValue = document.getFieldValue(aclField, Object.class, ignoreMissing);

        if (aclValue == null) {
            if (ignoreMissing) {
                return document;
            }
            throw new IllegalArgumentException("field [" + aclField + "] not present as part of path [" + aclField + "]");
        }

        // Check if routing already exists
        if (!overrideExisting && document.hasField(targetField)) {
            return document;
        }

        String routingValue = generateRoutingValue(aclValue.toString());
        document.setFieldValue(targetField, routingValue);

        return document;
    }

    private String generateRoutingValue(String aclValue) {
        // Use MurmurHash3 for consistent hashing
        byte[] bytes = aclValue.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());

        // Convert to base64 for routing value
        byte[] hashBytes = new byte[16];
        System.arraycopy(longToBytes(hash.h1), 0, hashBytes, 0, 8);
        System.arraycopy(longToBytes(hash.h2), 0, hashBytes, 8, 8);

        return BASE64_ENCODER.encodeToString(hashBytes);
    }

    private byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public AclRoutingProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String aclField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "acl_field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "_routing");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean overrideExisting = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "override_existing", true);

            return new AclRoutingProcessor(processorTag, description, aclField, targetField, ignoreMissing, overrideExisting);
        }
    }
}
