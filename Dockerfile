FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app ./cmd/api

FROM alpine:3.22
COPY --from=build /app /app
EXPOSE 8080
ENTRYPOINT ["/app"]

