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
import java.util.HashSet;
import java.util.Set;

/**
 * Response for Hunspell cache information.
 *
 * @opensearch.internal
 */
public class HunspellCacheInfoResponse extends ActionResponse implements ToXContentObject {

    private final int totalCachedCount;
    private final int packageBasedCount;
    private final int traditionalLocaleCount;
    private final Set<String> packageBasedKeys;
    private final Set<String> traditionalLocaleKeys;

    public HunspellCacheInfoResponse(
        int totalCachedCount,
        int packageBasedCount,
        int traditionalLocaleCount,
        Set<String> packageBasedKeys,
        Set<String> traditionalLocaleKeys
    ) {
        this.totalCachedCount = totalCachedCount;
        this.packageBasedCount = packageBasedCount;
        this.traditionalLocaleCount = traditionalLocaleCount;
        this.packageBasedKeys = packageBasedKeys;
        this.traditionalLocaleKeys = traditionalLocaleKeys;
    }

    public HunspellCacheInfoResponse(StreamInput in) throws IOException {
        super(in);
        this.totalCachedCount = in.readVInt();
        this.packageBasedCount = in.readVInt();
        this.traditionalLocaleCount = in.readVInt();
        this.packageBasedKeys = new HashSet<>(in.readStringList());
        this.traditionalLocaleKeys = new HashSet<>(in.readStringList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalCachedCount);
        out.writeVInt(packageBasedCount);
        out.writeVInt(traditionalLocaleCount);
        out.writeStringCollection(packageBasedKeys);
        out.writeStringCollection(traditionalLocaleKeys);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total_cached_count", totalCachedCount);
        builder.field("package_based_count", packageBasedCount);
        builder.field("traditional_locale_count", traditionalLocaleCount);
        builder.array("package_based_keys", packageBasedKeys.toArray(new String[0]));
        builder.array("traditional_locale_keys", traditionalLocaleKeys.toArray(new String[0]));
        builder.endObject();
        return builder;
    }

    public int getTotalCachedCount() {
        return totalCachedCount;
    }

    public int getPackageBasedCount() {
        return packageBasedCount;
    }

    public int getTraditionalLocaleCount() {
        return traditionalLocaleCount;
    }

    public Set<String> getPackageBasedKeys() {
        return packageBasedKeys;
    }

    public Set<String> getTraditionalLocaleKeys() {
        return traditionalLocaleKeys;
    }
}