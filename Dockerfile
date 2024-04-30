FROM golang:alpine AS builder

RUN apk add --no-cache wget

WORKDIR /app

# Copy the entire project directory into the container
COPY . .

# Change directory to the cmd folder
WORKDIR /app/cmd

# Download dependencies
RUN go mod download

# Build the Go application
RUN go build -o main .

# Switch back to the root directory
WORKDIR /app

ENTRYPOINT ["/app/cmd/main"]