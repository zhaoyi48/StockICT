FROM python:3.11

ENV TZ=Asia/Shanghai

WORKDIR /src

COPY /src /src
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt



VOLUME [ "/config" ]

ENTRYPOINT ["python3", "scheduleTool.py"]