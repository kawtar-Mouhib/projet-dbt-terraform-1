# Base image
FROM ghcr.io/dbt-labs/dbt-bigquery:1.8.2

ARG user=dbtuser
ARG group=dbtusers
ARG uid=1000
ARG gid=1000
ARG APP_DIR=/app/dbt

ENV DBT_PROFILES_DIR=$APP_DIR

WORKDIR /app

# Install cloud-sdk
RUN apt-get update && \
    apt-get install -y curl gnupg apt-transport-https ca-certificates && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y && \
    apt-get -y -q clean all &&\
    rm -rf /var/lib/apt/lists*

# Copy files
COPY dbt $APP_DIR
COPY script.sh /app/

# Create group and user
RUN addgroup --gid ${gid} ${group} \
    && adduser --disabled-password -gecos "" --uid ${uid} --ingroup ${group} ${user} \
    && chown -R ${user} /app \
    && chmod u+x /app/script.sh

# Switch to non-root user
USER ${user} 

WORKDIR $APP_DIR

# Set entrypoint
ENTRYPOINT ["../script.sh"]
