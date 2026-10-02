FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /rag-api ./cmd/rag-api

FROM alpine:3.20
COPY --from=build /rag-api /usr/local/bin/rag-api
EXPOSE 8080
ENTRYPOINT ["rag-api"]
