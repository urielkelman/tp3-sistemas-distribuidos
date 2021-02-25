FROM python:3.8
COPY tp2_utils_package /tp2_utils_package
COPY requirements.txt .
COPY requirements-travis.txt .
COPY .coveralls.yml .
COPY .coveragerc .
RUN pip install -r requirements.txt
RUN pip install -r requirements-travis.txt
RUN export PYTHONPATH="${PYTHONPATH}:tp2_utils_package"
CMD pytest --cov=. tp2_utils_package && coveralls