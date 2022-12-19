/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.opensearch.ExceptionsHelper;
import org.opensearch.authn.DefaultObjectMapper;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

public class SecurityDynamicConfiguration<T> implements ToXContent {

    private static final TypeReference<HashMap<String, Object>> typeRefMSO = new TypeReference<HashMap<String, Object>>() {
    };

    @JsonIgnore
    private final Map<String, T> centries = new HashMap<>();
    private long seqNo = -1;
    private long primaryTerm = -1;
    private CType ctype;
    private int version = -1;

    public static <T> SecurityDynamicConfiguration<T> empty() {
        return new SecurityDynamicConfiguration<T>();
    }

    public static <T> SecurityDynamicConfiguration<T> fromJson(String json, CType ctype, int version, long seqNo, long primaryTerm)
        throws IOException {
        SecurityDynamicConfiguration<T> sdc = null;
        if (ctype != null) {
            final Class<?> implementationClass = ctype.getImplementationClass().get(version);
            if (implementationClass == null) {
                throw new IllegalArgumentException("No implementation class found for " + ctype + " and config version " + version);
            }
            sdc = DefaultObjectMapper.readValue(
                json,
                DefaultObjectMapper.getTypeFactory().constructParametricType(SecurityDynamicConfiguration.class, implementationClass)
            );
            validate(sdc, version, ctype);

        } else {
            sdc = new SecurityDynamicConfiguration<T>();
        }

        sdc.ctype = ctype;
        sdc.seqNo = seqNo;
        sdc.primaryTerm = primaryTerm;
        sdc.version = version;

        return sdc;
    }

    public static void validate(SecurityDynamicConfiguration sdc, int version, CType ctype) throws IOException {
        if (version < 2 && sdc.get_meta() != null) {
            throw new IOException("A version of " + version + " can not have a _meta key for " + ctype);
        }

        if (version >= 2 && sdc.get_meta() == null) {
            throw new IOException("A version of " + version + " must have a _meta key for " + ctype);
        }
    }

    public static <T> SecurityDynamicConfiguration<T> fromNode(JsonNode json, CType ctype, int version, long seqNo, long primaryTerm)
        throws IOException {
        return fromJson(DefaultObjectMapper.writeValueAsString(json, false), ctype, version, seqNo, primaryTerm);
    }

    // for Jackson
    private SecurityDynamicConfiguration() {
        super();
    }

    private Meta _meta;

    public Meta get_meta() {
        return _meta;
    }

    public void set_meta(Meta _meta) {
        this._meta = _meta;
    }

    @JsonAnySetter
    void setCEntries(String key, T value) {
        putCEntry(key, value);
    }

    @JsonAnyGetter
    public Map<String, T> getCEntries() {
        return centries;
    }

    @JsonIgnore
    public void clearHashes() {
        for (Entry<String, T> entry : centries.entrySet()) {
            if (entry.getValue() instanceof Hashed) {
                ((Hashed) entry.getValue()).clearHash();
            }
        }
    }

    public void removeOthers(String key) {
        T tmp = this.centries.get(key);
        this.centries.clear();
        this.centries.put(key, tmp);
    }

    @JsonIgnore
    public T putCEntry(String key, T value) {
        return centries.put(key, value);
    }

    @JsonIgnore
    public void putCObject(String key, Object value) {
        centries.put(key, (T) value);
    }

    @JsonIgnore
    public T getCEntry(String key) {
        return centries.get(key);
    }

    @JsonIgnore
    public boolean exists(String key) {
        return centries.containsKey(key);
    }

    @JsonIgnore
    public BytesReference toBytesReference() throws IOException {
        return XContentHelper.toXContent(this, XContentType.JSON, false);
    }

    @Override
    public String toString() {
        return "SecurityDynamicConfiguration [seqNo="
            + seqNo
            + ", primaryTerm="
            + primaryTerm
            + ", ctype="
            + ctype
            + ", version="
            + version
            + ", centries="
            + centries
            + ", getImplementingClass()="
            + getImplementingClass()
            + "]";
    }

    @Override
    @JsonIgnore
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final boolean omitDefaults = params != null && params.paramAsBoolean("omit_defaults", false);
        return builder.map(DefaultObjectMapper.readValue(DefaultObjectMapper.writeValueAsString(this, omitDefaults), typeRefMSO));
    }

    @Override
    @JsonIgnore
    public boolean isFragment() {
        return false;
    }

    @JsonIgnore
    public long getSeqNo() {
        return seqNo;
    }

    @JsonIgnore
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    @JsonIgnore
    public CType getCType() {
        return ctype;
    }

    @JsonIgnore
    public void setCType(CType ctype) {
        this.ctype = ctype;
    }

    @JsonIgnore
    public int getVersion() {
        return version;
    }

    @JsonIgnore
    public Class<?> getImplementingClass() {
        return ctype == null ? null : ctype.getImplementationClass().get(getVersion());
    }

    @JsonIgnore
    public SecurityDynamicConfiguration<T> deepClone() {
        try {
            return fromJson(DefaultObjectMapper.writeValueAsString(this, false), ctype, version, seqNo, primaryTerm);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    @JsonIgnore
    public void remove(String key) {
        centries.remove(key);

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean add(SecurityDynamicConfiguration other) {
        if (other.ctype == null || !other.ctype.equals(this.ctype)) {
            return false;
        }

        if (other.getImplementingClass() == null || !other.getImplementingClass().equals(this.getImplementingClass())) {
            return false;
        }

        if (other.version != this.version) {
            return false;
        }

        this.centries.putAll(other.centries);
        return true;
    }

    @JsonIgnore
    @SuppressWarnings({ "rawtypes" })
    public boolean containsAny(SecurityDynamicConfiguration other) {
        return !Collections.disjoint(this.centries.keySet(), other.centries.keySet());
    }
}
