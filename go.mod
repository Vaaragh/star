module github.com/c12s/star

go 1.21.3

toolchain go1.21.4

require (
	github.com/c12s/kuiper v1.0.0
	github.com/c12s/magnetar v1.0.0
	github.com/hashicorp/serf v0.10.1
	github.com/nats-io/nats.go v1.31.0
	github.com/shirou/gopsutil v3.21.11+incompatible
)

require (
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/hashicorp/memberlist v0.5.0 // indirect
	github.com/miekg/dns v1.1.41 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
)

require (
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/nats-io/nkeys v0.4.5 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	golang.org/x/crypto v0.19.0 // indirect
	golang.org/x/net v0.21.0 // indirect
	golang.org/x/sys v0.17.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/grpc v1.63.2
	google.golang.org/protobuf v1.33.0
)

replace github.com/c12s/magnetar => ../magnetar

replace github.com/c12s/kuiper => ../kuiper

replace github.com/c12s/oort => ../oort
