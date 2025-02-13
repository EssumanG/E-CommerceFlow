FROM python:3.12

WORKDIR /root/flaskapp

COPY . /root/flaskapp/

RUN cd root/flaskapp; la; cat requirements.txt; pip install -r requirements.txt 

RUN apt-get -y update && apt-get -y install vim

CMD ["hypercorn", "--bind", "0.0.0.0:10000", "main:app"]