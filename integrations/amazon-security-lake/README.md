### Amazon Security Lake integration - Logstash

Follow the [Wazuh indexer integration using Logstash](https://documentation.wazuh.com/current/integrations-guide/opensearch/index.html#wazuh-indexer-integration-using-logstash)
to install `Logstash` and the `logstash-input-opensearch` plugin.

> RPM: https://www.elastic.co/guide/en/logstash/current/installing-logstash.html#_yum
```markdown

# Install plugins (logstash-output-s3 is already installed)
sudo /usr/share/logstash/bin/logstash-plugin install logstash-input-opensearch

# Copy certificates
mkdir -p /etc/logstash/wi-certs/
cp /etc/wazuh-indexer/certs/root-ca.pem /etc/logstash/wi-certs/root-ca.pem
chown logstash:logstash /etc/logstash/wi-certs/root-ca.pem

# Configuring new indexes
SKIP

# Configuring a pipeline

# Keystore
## Prepare keystore
set +o history
echo 'LOGSTASH_KEYSTORE_PASS="123456"'| sudo tee /etc/sysconfig/logstash
export LOGSTASH_KEYSTORE_PASS=123456
set -o history
sudo chown root /etc/sysconfig/logstash
sudo chmod 600 /etc/sysconfig/logstash
sudo systemctl start logstash

## Create keystore
sudo -E /usr/share/logstash/bin/logstash-keystore --path.settings /etc/logstash create

## Store Wazuh indexer credentials (admin user)
sudo -E /usr/share/logstash/bin/logstash-keystore --path.settings /etc/logstash add WAZUH_INDEXER_USERNAME
sudo -E /usr/share/logstash/bin/logstash-keystore --path.settings /etc/logstash add WAZUH_INDEXER_PASSWORD

# Pipeline
sudo touch /etc/logstash/conf.d/wazuh-s3.conf
# Replace with cp /vagrant/wazuh-s3.conf /etc/logstash/conf.d/wazuh-s3.conf
sudo systemctl stop logstash
sudo -E /usr/share/logstash/bin/logstash -f /etc/logstash/conf.d/wazuh-s3.conf --path.settings /etc/logstash/
    |- Success: `[INFO ][logstash.agent           ] Pipelines running ...`

# Start Logstash
sudo systemctl enable logstash
sudo systemctl start logstash
```