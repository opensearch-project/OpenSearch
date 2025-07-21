# Cross Cluster Search (CCS) environment
This environment enables the deployment of a Wazuh Indexer cluster with Cross Cluster Search configuration, using Vagrant and Virtualbox (or other supported providers).

It also generates the node's required certificates using the `wazuh-certs-tool` and copy them to each node's `/home/vagrant`
directory, leaving a copy in `tools/ccs`.

For the development of this environment, we have based it on [Wazuh documentation](https://wazuh.com/blog/managing-multiple-wazuh-clusters-with-cross-cluster-search/)

### Prerequisites

1. Download and install Vagrant ([source](https://developer.hashicorp.com/vagrant/downloads))
2. Install virtualbox ([source](https://www.virtualbox.org/wiki/Downloads))

> [!Note]
> If instead of virtualbox you want to use another provider like libvirt, the variable on the second line of the Vagrantfile should be changed to the name of the desired provider.

## Wazuh Version Configuration

The Wazuh version is set in the first line of the `Vagrantfile` within the variable `version`. You can change it to your desired version, for example:

```
version = "4.12.0"
```

This version is passed to the `node-start.sh` script during provisioning.

## Infrastructure Overview
The environment includes the following nodes:

- ccs: Main control node (Cross Cluster Search)
- cluster_a: Cluster A node
- cluster_b: Cluster B node

Each node is configured with its IP address, hostname, and system resources (RAM, CPUs).

## Requirements:
| Node      | RAM      | CPU        |
|-----------|----------|------------|
| ccs       | 4 GB     | 4 cores    |
| cluster_a | 4 GB     | 4 cores    |
| cluster_b | 4 GB     | 4 cores    |


## Usage

1. Navigate to the environment's root directory
   ```bash
   cd tools
   ```
2. Initialize the environment
   ```bash
   vagrant up
   ```

> [!Note]
> The process of starting all the nodes and configuring them may take approximately 20 minutes.

3. Connect to the different systems
   ```bash
   vagrant ssh ccs/cluster_a/cluster_b
   ```


## Test the Cross-Cluster Search 
Perform the following steps on the Wazuh dashboard to enable Cross-Cluster Search from the CCS environment to the remote clusters.

1. Log in to the Wazuh dashboard using the login credentials:
```
URL: https://192.168.56.10
Username: admin
Password: admin
```

2. Test that the remote clusters are connected by running the following API call:
> [!Note] Note: Change the Wazuh indexer name highlighted to match the cluster being tested.

```
GET ca-wazuh-indexer-1:wazuh-alerts-*/_search
```

Output
``` json
{
  "took": 833,
  "timed_out": false,
  "_shards": {
    "total": 6,
    "successful": 6,
    "skipped": 0,
    "failed": 0
  },
  "_clusters": {
    "total": 1,
    "successful": 1,
    "skipped": 0
  },
  "hits": {
    "total": {
      "value": 221,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": [
      {
        "_index": "ca-wazuh-indexer-1:wazuh-alerts-4.x-2024.08.25",
        "_id": "BZ40i5EB-SZRRdc_oF6H",
        "_score": 1,
        "_source": {
          "predecoder": {
            "hostname": "ccs",
            "program_name": "systemd",
            "timestamp": "Aug 25 21:22:35"
          },
          "agent": {
            "name": "cluster-a",
            "id": "000"
          },
          "manager": {
            "name": "cluster-a"
          },
          "rule": {
            "firedtimes": 1,
            "mail": false,
            "level": 5,
            "description": "Systemd: System time has been changed.",
            "groups": [
              "local",
              "systemd"
            ],
            "id": "40705",
            "gpg13": [
              "4.3"
            ],
            "gdpr": [
              "IV_35.7.d"
            ]
          },
          "decoder": {
            "name": "systemd"
          },
          "full_log": "Aug 25 21:22:35 ccs systemd: Time has been changed",
          "input": {
            "type": "log"
          },
          "@timestamp": "2024-08-25T20:22:37.091Z",
          "location": "/var/log/messages",
          "id": "1724617357.562701",
          "timestamp": "2024-08-25T21:22:37.091+0100"
        }
      },
...
```

### Configure the `wazuh-alerts-*` index pattern
1. Select **☰** > **Dashboard management** > **Dashboard Management**  > **Index patterns** and select **Create index pattern** to add the index patterns for the remote clusters.

2. Add the index pattern name using the format `*:wazuh-alerts-*` and select Next step. The wildcard ‘`*`‘ matches all indexers in the remote Wazuh clusters.
   
3. Select **@timestamp** as the primary time field.

4. Select **Create index pattern** to create the index pattern.

5. Select **☰** > **Dashboard management** > **App Settings**  > **General** and set the default index pattern for alerts to `*:wazuh-alerts-*` in the **Index pattern** field.

6. Select the `*:wazuh-alerts-*` index pattern and toggle the API between Cluster A and B to view alerts from both remote clusters.

### Configure the `wazuh-states-vulnerabilities*` index pattern
1. Select **☰** > **Dashboard management** > **Dashboard Management**  > **Index patterns** and select **Create index pattern** to add the index patterns for the remote clusters.

2. Add the index pattern name using the format `*:wazuh-states-vulnerabilities-*` and select **Next step**.  The wildcard ‘`*`‘ matches all indexers in the remote Wazuh clusters.

3. Select **package.installed** as the primary time field. This will show you when the vulnerable package was installed.

4. Select **Create index pattern** to create the index pattern.

5. Select **☰** > **Dashboard management** > **App Settings**  > **Vulnerabilities** and set the default index pattern for vulnerabilities to `*:wazuh-states-vulnerabilities-*` in the **Index pattern** field.
## Cleanup

After the testing session is complete you can stop or destroy the environment as you wish:

- Stop the environment:
  ```bash
  vagrant halt
  ```
- Destroy the environment:
  ```bash
  vagrant destroy -f
  ```
