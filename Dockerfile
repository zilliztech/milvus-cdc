FROM golang:1.24.2 AS builder

ENV CGO_ENABLED=1

# Add git to the builder stage
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

ARG GIT_COMMIT_ARG
ENV CDC_GIT_COMMIT=${GIT_COMMIT_ARG}

WORKDIR /app
COPY . .

# Modify this line to pass CDC_GIT_COMMIT to make
RUN cd server && make build GIT_COMMIT=${CDC_GIT_COMMIT} && mv ../bin/cdc /app/milvus-cdc

FROM debian:bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/milvus-cdc ./
COPY --from=builder /app/server/configs ./configs

EXPOSE 8444

CMD ["/bin/bash", "-c", "cat /app/configs/cdc.yaml;/app/milvus-cdc"]