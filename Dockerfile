FROM python:3.7-slim

ENV TZ='Pacific/Auckland'

RUN apt-get update && apt-get install -y unixodbc-dev gcc g++ libspatialindex-dev python-rtree

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY utils.py process_limits.py process_allocation.py process_waps.py aggregate_allocation.py main.py use_type_mapping_table.csv, use_types_priorities.csv ./

CMD ["python", "main.py", "parameters.yml"]
