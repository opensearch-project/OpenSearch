# Security Plugin Integration

The Arrow Flight RPC plugin integrates with the OpenSearch Security plugin to provide secure streaming transport with TLS encryption. The Flight transport operates on a dedicated port (default `9400-9500`) for internal node-to-node communication, similar to the standard transport on port `9300`.

> **⚠️ WARNING:** Always use `FLIGHT-SECURE` with `flight.ssl.enable: true` when the security plugin is installed. Using `FLIGHT` (non-secure) mode bypasses transport-layer encryption and allows unencrypted node-to-node communication. See [Transport Modes](#transport-modes-flight-vs-flight-secure) for details.

## Configuration

Add these settings to `opensearch.yml`:

```yaml
# Enable streaming transport
opensearch.experimental.feature.transport.stream.enabled: true

# Use secure Flight as default transport
transport.stream.type.default: FLIGHT-SECURE

# Enable Flight TLS
flight.ssl.enable: true
```

## Transport Modes: FLIGHT vs FLIGHT-SECURE

The plugin provides two transport modes. Choosing the correct mode is critical for security:

| Mode | TLS | Use Case |
|------|-----|----------|
| `FLIGHT-SECURE` | Encrypted (mTLS) | **Production — always use this when security is enabled** |
| `FLIGHT` | Plaintext | Development/testing only, with no security plugin |

### FLIGHT-SECURE (Recommended)

`FLIGHT-SECURE` enables TLS encryption on the Flight transport port, providing the same security guarantees as the standard transport port (`9300`) with `plugins.security.ssl.transport.enabled: true`. This is the only mode that should be used when the OpenSearch Security plugin is installed.

### FLIGHT (Non-Secure)

> **⚠️ WARNING:** `FLIGHT` mode transmits all inter-node traffic in plaintext. Do not use this mode when the security plugin is installed or when the cluster handles sensitive data.

`FLIGHT` mode starts the Flight transport without TLS. When used with the security plugin installed, this creates a misconfiguration where:
- Node-to-node communication on the Flight port is unencrypted
- Any client that can reach the Flight port can send requests without TLS-based node identity verification

This mode exists only for development and testing environments where the security plugin is not installed.

## Security Plugin Setup

Install and configure the security plugin:

```bash
# Install security plugin
bin/opensearch-plugin install opensearch-security

# Setup demo configuration
plugins/opensearch-security/tools/install_demo_configuration.sh
```

## Secure Configuration Checklist

When deploying with the security plugin, verify all three settings are present:

```yaml
# 1. Enable the experimental streaming transport feature
opensearch.experimental.feature.transport.stream.enabled: true

# 2. Select the secure transport mode (NOT "FLIGHT")
transport.stream.type.default: FLIGHT-SECURE

# 3. Enable TLS on the Flight port
flight.ssl.enable: true
```

Missing any of these settings results in an insecure configuration. Specifically:
- Omitting `flight.ssl.enable: true` — TLS defaults to **disabled** (`false`), leaving the Flight port unencrypted
- Using `FLIGHT` instead of `FLIGHT-SECURE` — starts the transport without TLS regardless of other settings

## Role-Based Access Control

The Flight transport supports all security plugin features:
- Index-level permissions
- Document-level security (DLS)
- Field-level security (FLS)
- Action-level permissions

These security controls are enforced at the action filter layer (SecurityFilter) and apply regardless of the transport type. RBAC, DLS, and FLS protections work correctly when the cluster is properly configured with `FLIGHT-SECURE`.

## How Security Is Enforced

The Flight transport reuses the same security enforcement as the standard Netty transport:

1. **Connection layer (TLS/mTLS):** When `FLIGHT-SECURE` is used with `flight.ssl.enable: true`, mutual TLS authenticates nodes at the connection level — the same model as port `9300` with `plugins.security.ssl.transport.enabled: true`.
2. **Action layer (SecurityFilter):** RBAC, DLS, and FLS are enforced by the security plugin's action filter, which runs independently of the transport channel type.

Both layers are required for a fully secure deployment. TLS ensures only trusted nodes can connect; the action filter ensures authorized access to data.
