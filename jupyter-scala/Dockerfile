FROM jupyter/all-spark-notebook:spark-3.3.0

USER root

# Install Scala kernel
RUN pip install --no-cache-dir spylon-kernel pymongo && \
    python -m spylon_kernel install

