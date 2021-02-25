FROM python:3.8
COPY . /repo
WORKDIR /repo
RUN pip install -r requirements.txt
RUN pip install -r requirements-travis.txt
CMD  export PYTHONPATH="${PYTHONPATH}:tp2_utils_package" && pytest --cov=. && coveralls