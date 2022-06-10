FROM golang:1.18-alpine as build

#RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN GOOS=linux CGO_ENABLED=0 go build -o /app/skvcli github.com/sonald/skv/cmd/skvcli
RUN GOOS=linux CGO_ENABLED=0 go build -o /app/skv github.com/sonald/skv/cmd/skv

COPY ./scripts/node.yaml /app

FROM gcr.io/distroless/base-debian11 as production

COPY --from=build /app/skv /skv
COPY --from=build /app/skvcli /skvcli
COPY --from=build /app/node.yaml /node.yaml

EXPOSE 9528
EXPOSE 9527

CMD ["/skv", "-B", "-c", "node.yaml"]
