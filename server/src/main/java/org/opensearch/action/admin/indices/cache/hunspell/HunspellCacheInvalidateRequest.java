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
import org.opensearch.core.common.Strings;
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
        ActionRequestValidationException e = null;
        
        // Reject empty/blank strings with clear error messages
        if (packageId != null && !Strings.hasText(packageId)) {
            e = new ActionRequestValidationException();
            e.addValidationError("'package_id' cannot be empty or blank");
        }
        if (locale != null && !Strings.hasText(locale)) {
            if (e == null) e = new ActionRequestValidationException();
            e.addValidationError("'locale' cannot be empty or blank");
        }
        if (cacheKey != null && !Strings.hasText(cacheKey)) {
            if (e == null) e = new ActionRequestValidationException();
            e.addValidationError("'cache_key' cannot be empty or blank");
        }
        
        // If any blank validation errors, return early
        if (e != null) {
            return e;
        }
        
        // Count how many modes are specified
        int modeCount = 0;
        if (invalidateAll) modeCount++;
        if (packageId != null) modeCount++;
        if (cacheKey != null) modeCount++;
        
        if (modeCount == 0) {
            e = new ActionRequestValidationException();
            e.addValidationError("Either 'package_id', 'cache_key', or 'invalidate_all' must be specified");
        } else if (modeCount > 1) {
            e = new ActionRequestValidationException();
            if (invalidateAll && (packageId != null || cacheKey != null)) {
                e.addValidationError("'invalidate_all' cannot be combined with 'package_id' or 'cache_key'");
            } else {
                e.addValidationError("Only one of 'package_id' or 'cache_key' can be specified, not both");
            }
        }
        
        // locale is only valid with package_id
        if (locale != null && packageId == null) {
            if (e == null) e = new ActionRequestValidationException();
            e.addValidationError("'locale' can only be specified together with 'package_id'");
        }
        
        return e;
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