FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app ./cmd/api
RUN CGO_ENABLED=0 go build -o /migrate ./cmd/migrate

FROM build AS test
RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 go test -race ./...

FROM alpine:3.22
COPY --from=build /app /app
COPY --from=build /migrate /migrate
COPY migrations /migrations
EXPOSE 8080
ENTRYPOINT ["/app"]
