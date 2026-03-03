/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.hunspell;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for Hunspell cache invalidation.
 * 
 * <p>Supports three modes:
 * <ul>
 *   <li>Invalidate by package_id (clears all locales for a package)</li>
 *   <li>Invalidate by package_id + locale (clears specific entry)</li>
 *   <li>Invalidate by cache_key (direct key)</li>
 *   <li>Invalidate all (invalidateAll = true)</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class HunspellCacheInvalidateRequest extends ActionRequest {

    private String packageId;
    private String locale;
    private String cacheKey;
    private boolean invalidateAll;

    public HunspellCacheInvalidateRequest() {
    }

    public HunspellCacheInvalidateRequest(StreamInput in) throws IOException {
        super(in);
        this.packageId = in.readOptionalString();
        this.locale = in.readOptionalString();
        this.cacheKey = in.readOptionalString();
        this.invalidateAll = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(packageId);
        out.writeOptionalString(locale);
        out.writeOptionalString(cacheKey);
        out.writeBoolean(invalidateAll);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (!invalidateAll && packageId == null && cacheKey == null) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("Either 'package_id', 'cache_key', or 'invalidate_all' must be specified");
            return e;
        }
        return null;
    }

    public String getPackageId() {
        return packageId;
    }

    public HunspellCacheInvalidateRequest setPackageId(String packageId) {
        this.packageId = packageId;
        return this;
    }

    public String getLocale() {
        return locale;
    }

    public HunspellCacheInvalidateRequest setLocale(String locale) {
        this.locale = locale;
        return this;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public HunspellCacheInvalidateRequest setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    public boolean isInvalidateAll() {
        return invalidateAll;
    }

    public HunspellCacheInvalidateRequest setInvalidateAll(boolean invalidateAll) {
        this.invalidateAll = invalidateAll;
        return this;
    }
}