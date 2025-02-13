FROM python:3.12

WORKDIR /root/flaskapp

COPY . /root/flaskapp/

EXPOSE 8000

RUN cd root/flaskapp; la; cat requirements.txt; pip install -r requirements.txt 

RUN apt-get -y update && apt-get -y install vim

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "main:app"]