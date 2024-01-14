FROM python:3.9

WORKDIR /

COPY docker_setup/python_packages.txt /Docker/
RUN pip3 install -r /Docker/python_packages.txt

COPY docker_setup/duckdb_extensions.py /Docker/
RUN python3 /Docker/duckdb_extensions.py

WORKDIR /stackt/dbt_project

ENTRYPOINT [ "bash" ]
