/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

    public static void validate(SecurityDynamicConfiguration<?> sdc, int version, CType ctype) throws IOException {
        if (version < 2 && sdc.getConfigVersion() != null) {
            throw new IOException("A version of " + version + " can not have a _meta key for " + ctype);
        }

        if (version >= 2 && sdc.getConfigVersion() == null) {
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

    private ConfigVersion _configVersion;

    public ConfigVersion getConfigVersion() {
        return _configVersion;
    }

    public void setConfigVersion(ConfigVersion _configVersion) {
        this._configVersion = _configVersion;
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
    public T putCEntry(String key, T value) {
        return centries.put(key, value);
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
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
}
