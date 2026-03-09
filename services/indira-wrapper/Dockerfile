FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.work* ./
COPY go.mod go.sum ./
COPY api/ api/
COPY pkg/ pkg/
COPY services/indira-wrapper/ services/indira-wrapper/

RUN cd services/indira-wrapper && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/indira-wrapper ./cmd

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=Asia/Kolkata

WORKDIR /app
COPY --from=builder /bin/indira-wrapper .

EXPOSE 50056
CMD ["./indira-wrapper"]
