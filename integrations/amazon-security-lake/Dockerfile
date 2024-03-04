# MULTI-STAGE build

FROM python:3.9 as builder
# Create a virtualenv for dependencies. This isolates these packages from
# system-level packages.
RUN python3 -m venv /env
# Setting these environment variables are the same as running
# source /env/bin/activate.
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH
# Copy the application's requirements.txt and run pip to install all
# dependencies into the virtualenv.
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt


FROM python:3.9
ENV LOGSTASH_KEYSTORE_PASS="SecretPassword"
# Add the application source code.
COPY --chown=logstash:logstash . /home/app
# Add execution persmissions.
RUN chmod a+x /home/app/run.py
# Copy the application's dependencies.
COPY --from=builder /env /env

# Install Logstash
RUN apt-get update && apt-get install -y iputils-ping wget gpg apt-transport-https
RUN wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | gpg --dearmor -o /usr/share/keyrings/elastic-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/elastic-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-8.x.list && \
    apt-get update && apt install -y logstash
# Install logstash-input-opensearch plugin.
RUN /usr/share/logstash/bin/logstash-plugin install logstash-input-opensearch
# Copy the Logstash's ingestion pipelines.
COPY --chown=logstash:logstash logstash/pipeline /usr/share/logstash/pipeline
# Grant logstash ownership over its files
RUN chown --recursive logstash:logstash /usr/share/logstash /etc/logstash /var/log/logstash /var/lib/logstash

USER logstash
# Copy and run the setup.sh script to create and configure a keystore for Logstash.
COPY --chown=logstash:logstash logstash/setup.sh /usr/share/logstash/bin/setup.sh
RUN bash /usr/share/logstash/bin/setup.sh