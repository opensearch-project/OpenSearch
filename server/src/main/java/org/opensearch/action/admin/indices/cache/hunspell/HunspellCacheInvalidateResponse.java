/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.hunspell;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for Hunspell cache invalidation action.
 *
 * @opensearch.internal
 */
public class HunspellCacheInvalidateResponse extends ActionResponse implements ToXContentObject {

    private final boolean acknowledged;
    private final int invalidatedCount;
    private final String packageId;
    private final String locale;
    private final String cacheKey;

    public HunspellCacheInvalidateResponse(boolean acknowledged, int invalidatedCount, String packageId, String locale, String cacheKey) {
        this.acknowledged = acknowledged;
        this.invalidatedCount = invalidatedCount;
        this.packageId = packageId;
        this.locale = locale;
        this.cacheKey = cacheKey;
    }

    public HunspellCacheInvalidateResponse(StreamInput in) throws IOException {
        super(in);
        this.acknowledged = in.readBoolean();
        this.invalidatedCount = in.readInt();
        this.packageId = in.readOptionalString();
        this.locale = in.readOptionalString();
        this.cacheKey = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        out.writeInt(invalidatedCount);
        out.writeOptionalString(packageId);
        out.writeOptionalString(locale);
        out.writeOptionalString(cacheKey);
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public int getInvalidatedCount() {
        return invalidatedCount;
    }

    public String getPackageId() {
        return packageId;
    }

    public String getLocale() {
        return locale;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", acknowledged);
        builder.field("invalidated_count", invalidatedCount);
        if (packageId != null) {
            builder.field("package_id", packageId);
        }
        if (locale != null) {
            builder.field("locale", locale);
        }
        if (cacheKey != null) {
            builder.field("cache_key", cacheKey);
        }
        builder.endObject();
        return builder;
    }
}