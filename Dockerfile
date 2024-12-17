# Use a dbt base image
FROM fishtownanalytics/dbt:latest

# Set working directory
WORKDIR /dbt

# Copy dbt project files into the container
COPY . /dbt

# Install dependencies (if any)
RUN dbt deps

# Default command
CMD ["dbt", "run"]
