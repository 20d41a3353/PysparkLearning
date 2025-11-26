
FROM adityakr123/pyspark-env


WORKDIR /app

# Copy all files from your current folder into the container’s /app directory
COPY . .


CMD ["spark-submit", "newTask.py"]
