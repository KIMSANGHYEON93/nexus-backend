FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 업데이트 (TimescaleDB 연결을 위한 라이브러리)
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# 파이썬 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사 (docker-compose의 볼륨 마운트로 로컬 개발 시에는 덮어씌워짐)
COPY ./src /app/src

EXPOSE 8000
