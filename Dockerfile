FROM python:3-alpine

ENV directory /app/rules
ENV port 9955


RUN cd /etc
RUN mkdir app
WORKDIR /etc/app
ADD *.py /etc/app/
ADD requirements.txt /etc/app/.
RUN pip install -r requirements.txt

CMD python /etc/app/rule_engine.py $directory $port


