# Security Plugin Integration

The Arrow Flight RPC plugin integrates with the OpenSearch Security plugin to provide secure streaming transport with TLS encryption.

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

## Security Plugin Setup

Install and configure the security plugin:

```bash
# Install security plugin
bin/opensearch-plugin install opensearch-security

# Setup demo configuration
plugins/opensearch-security/tools/install_demo_configuration.sh
```

## Role-Based Access Control

The Flight transport supports all security plugin features:
- Index-level permissions
- Document-level security (DLS)
- Field-level security (FLS)
- Action-level permissions