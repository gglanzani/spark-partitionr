FROM godatadriven/pyspark:latest

ADD . /code
WORKDIR /code

RUN pip install .[test]

CMD ["python", "-m", "pytest", "tests"]