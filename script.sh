#!/bin/bash
set -e
set -o pipefail
set -u

export DBT_PROFILES_DIR=~/.dbt
echo "DBT_PROFILES_DIR set to $DBT_PROFILES_DIR"

# Create the profiles.yml dynamically
mkdir -p ~/.dbt

cat <<EOF > ~/.dbt/profiles.yml
my-bigquery-db:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: ${DBT_PROJECT_ID}
      dataset: ${DBT_DATASET}
      threads: 4
EOF

echo "profiles.yml created successfully"


# read cloud run jobs arguments
dbt_build=$1
dbt_docs=$2

if [ $# -eq 0 ]; then
    echo "No arguments provided"
    exit 1
fi


echo "Installing dbt dependencies..."
dbt deps || { echo 'Failed to install dbt dependencies'; exit 1; }

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