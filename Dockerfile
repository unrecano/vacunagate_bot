FROM python:3.8-alpine

RUN mkdir /code
WORKDIR /code
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "main.py", "--stream"]