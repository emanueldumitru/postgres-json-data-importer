FROM python:3.10.9-slim

# Set environment variables that you want to expose (replace with your variables)
ENV POSTGRES_OLAP_HOST=localhost \
    POSTGRES_OLAP_DATABASE=onyxia_olap \
    POSTGRES_OLAP_PORT=5471 \
    POSTGRES_OLAP_USERNAME=postgres \
    POSTGRES_OLAP_PASSWORD=postgres

# Set the working directory inside the container
WORKDIR /app

# Copy your Python script into the container
COPY . .

RUN pip install -r requirements.txt


ENTRYPOINT ["python", "importer.py"]

# Define the command to run your script with parameters
CMD ["python", "importer.py"]