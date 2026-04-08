# OpenSearch Security Auto-Configuration

This feature automatically configures OpenSearch Security when running the development server with the security plugin.

## Usage

To run OpenSearch with security enabled:

```bash
./gradlew run -PinstalledPlugins="['opensearch-security']"
```

## What it does

When `opensearch-security` is included in the `installedPlugins` list, the build system automatically:

1. **Downloads demo certificates** from the security plugin repository:
   - `esnode.pem` / `esnode-key.pem` - Node certificates
   - `kirk.pem` / `kirk-key.pem` - Admin client certificates  
   - `root-ca.pem` - Root CA certificate

2. **Configures security settings** in `opensearch.yml`:
   - Enables SSL/TLS for HTTP and transport layers
   - Sets up certificate paths
   - Configures admin DN for kirk client
   - Enables security features (audit, snapshot restore, etc.)
   - Allows unsafe demo certificates for development

3. **Installs the security plugin** from Maven snapshots

## Security Settings Applied

The following security settings are automatically configured:

```yaml
plugins.security.ssl.transport.pemcert_filepath: esnode.pem
plugins.security.ssl.transport.pemkey_filepath: esnode-key.pem
plugins.security.ssl.transport.pemtrustedcas_filepath: root-ca.pem
plugins.security.ssl.transport.enforce_hostname_verification: false
plugins.security.ssl.http.enabled: true
plugins.security.ssl.http.pemcert_filepath: esnode.pem
plugins.security.ssl.http.pemkey_filepath: esnode-key.pem
plugins.security.ssl.http.pemtrustedcas_filepath: root-ca.pem
plugins.security.allow_unsafe_democertificates: true
plugins.security.allow_default_init_securityindex: true
plugins.security.authcz.admin_dn: "\n - CN=kirk,OU=client,O=client,L=test,C=de"
plugins.security.audit.type: internal_opensearch
plugins.security.enable_snapshot_restore_privilege: true
plugins.security.check_snapshot_restore_write_privileges: true
plugins.security.restapi.roles_enabled: ["all_access", "security_rest_api_access"]
plugins.security.system_indices.enabled: true
```

## Accessing the Secured Cluster

Once running, you can access OpenSearch at:
- HTTP: `https://localhost:9200` (note HTTPS)
- Default credentials: `admin:admin`

## Implementation Details

The auto-configuration is implemented in:
- `gradle/security-setup.gradle` - Security configuration logic
- `build.gradle` - Conditional application of security setup

The security setup is only applied when `opensearch-security` is detected in the `installedPlugins` property.