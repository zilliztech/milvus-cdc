FROM  golang:1.21 AS builder
ENV CGO_ENABLED=1
ARG GIT_COMMIT_ARG
ENV CDC_GIT_COMMIT=${GIT_COMMIT_ARG}
WORKDIR /app
COPY . .
RUN cd server && make build && mv ../bin/cdc /app/milvus-cdc

FROM debian:bullseye
WORKDIR /app
COPY --from=builder /app/milvus-cdc ./
COPY --from=builder /app/server/configs ./configs
EXPOSE 8444

CMD ["/bin/bash", "-c", "cat /app/configs/cdc.yaml;/app/milvus-cdc"]
