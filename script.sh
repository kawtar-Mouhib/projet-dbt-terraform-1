#!/bin/bash
set -e
set -o pipefail
set -u


# Create the profiles.yml dynamically
mkdir -p ~/.dbt

cat <<EOF > ~/.dbt/profiles.yml
my-bigquery-db:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: terraform-project-443816
      dataset: run-test
      threads: 4
EOF

echo "profiles.yml created successfully"

export DBT_PROFILES_DIR=~/.dbt
echo "DBT_PROFILES_DIR set to $DBT_PROFILES_DIR"

# read cloud run jobs arguments
dbt_build=$1
dbt_docs=$2

if [ $# -eq 0 ]; then
    echo "No arguments provided"
    exit 1
fi

# execute dbt build command (first argument)
$dbt_build

if [ $? -eq 0 ]
  then
    echo "dbt successfully run"
    # execute dbt docs command (second argument)
    $dbt_docs
    gsutil -m cp target/* gs://$DBT_DOCS_BUCKET/
    echo "Target folder copied to gcs"

  else
    echo "dbt failed"
    echo "hello "
    echo "test"
    echo "test 2"
    exit 1
fi