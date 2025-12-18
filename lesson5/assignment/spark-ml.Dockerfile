FROM apache/spark

USER root

RUN pip3 install --no-cache-dir \
        numpy \
        pandas \
        scipy \
        scikit-learn

USER spark
