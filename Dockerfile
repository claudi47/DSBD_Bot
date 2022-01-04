FROM python:3.10.1-alpine
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV BOT_TOKEN_DSBD=OTI2NTE0NjM4OTI4MDg1MTEz.Yc8x_w.6IWd7y2F24zoUWrzhB-w4_9K2IY

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN apk add build-base && apk add openssl && apk add libffi-dev
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./main.py" ]