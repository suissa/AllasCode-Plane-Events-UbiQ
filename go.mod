module github.com/nats-io/nats-server/v2

go 1.25.0

toolchain go1.25.9

require (
	github.com/antithesishq/antithesis-sdk-go v0.7.0-default-no-op
	github.com/google/go-tpm v0.9.8
	github.com/klauspost/compress v1.18.5
	github.com/minio/highwayhash v1.0.4
	github.com/nats-io/jwt/v2 v2.8.1
	github.com/nats-io/nats.go v1.51.0
	github.com/nats-io/nkeys v0.4.15
	github.com/nats-io/nuid v1.0.1
	github.com/quic-go/quic-go v0.59.0
	github.com/quic-go/webtransport-go v0.10.0
	golang.org/x/crypto v0.50.0
	golang.org/x/sys v0.43.0
	golang.org/x/time v0.15.0
)

require (
	github.com/dunglas/httpsfv v1.1.0 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/text v0.36.0 // indirect
)
