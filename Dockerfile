FROM prefecthq/prefect:2.8-python3.10

# Set environment variables
ENV DBT_DIR /dbt

# Set working directory
WORKDIR $DBT_DIR

# Copy requirements
COPY requirements.txt .

# Install requirements
RUN pip install -r requirements.txt

COPY prefect_qbi prefect_qbi/

