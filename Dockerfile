
FROM golang:alpine AS builder

RUN apk add --no-cache wget

WORKDIR /app

# COPY . .
COPY . /app
RUN go mod download

RUN go build -o main .

# FROM scratch

# COPY --from=builder /app/main /app/main
# COPY --from=builder /app ./app

ENTRYPOINT ["/app/main"]