FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/broker ./cmd/broker

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/broker /app/broker
COPY configs/broker.yaml /app/configs/broker.yaml
EXPOSE 5672 8080 9090
VOLUME ["/app/data"]
ENTRYPOINT ["/app/broker"]
CMD ["-config", "/app/configs/broker.yaml"]
