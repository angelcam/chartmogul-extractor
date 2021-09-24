FROM python:3.8-slim

ADD requirements.txt /requirements.txt

RUN python -m pip install -r requirements.txt

ADD src /code

WORKDIR /code

CMD ["python3", "-u", "main.py"]
