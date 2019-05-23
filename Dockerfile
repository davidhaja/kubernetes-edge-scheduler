FROM python:3
COPY . /custom-scheduler
WORKDIR /custom-scheduler
RUN pip3 install -r requirements.txt
CMD python3 -u main.py