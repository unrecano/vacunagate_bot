FROM python:3.8-alpine

RUN mkdir /code
WORKDIR /code
COPY . .
COPY docker/entrypoint.sh .
RUN pip install -r requirements.txt
RUN chmod +x entrypoint.sh
CMD "/code/entrypoint.sh"