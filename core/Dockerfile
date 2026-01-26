# build
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o core-go ./app/main.go

# run
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /src/core-go /core-go
ENV PORT=8000
EXPOSE 8000
ENTRYPOINT ["/core-go"]
