FROM  golang:1.18 AS builder
ENV CGO_ENABLED=1
WORKDIR /app
COPY . .
RUN go mod tidy
RUN cd server && go build -o /app/milvus-cdc main/main.go

FROM debian:bullseye
WORKDIR /app
COPY --from=builder /app/milvus-cdc ./
COPY --from=builder /app/server/configs ./configs
EXPOSE 8444

CMD ["/bin/bash", "-c", "cat /app/configs/cdc.yaml;/app/milvus-cdc"]
