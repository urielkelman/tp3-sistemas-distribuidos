FROM python:3.8
COPY tp2_utils_package /tp2_utils_package
COPY requirements.txt .
COPY requirements-travis.txt .
COPY .coveralls.yml .
COPY .coveragerc .
COPY .git .
COPY __init__.py .
RUN pip install -r requirements.txt
RUN pip install -r requirements-travis.txt
CMD  export PYTHONPATH="${PYTHONPATH}:tp2_utils_package" && pytest --cov=. && coveralls