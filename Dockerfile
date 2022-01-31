FROM python:3.10.1-alpine
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/usr/src/app

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN apk add build-base && apk add openssl && apk add libffi-dev && apk add chromium && apk add chromium-chromedriver && apk add librdkafka-dev
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./app/main.py" ]