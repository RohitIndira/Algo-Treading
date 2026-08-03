module github.com/RohitIndira/Algo-Treading/services/positions

go 1.25.0
toolchain go1.25.12

require (
	github.com/RohitIndira/Algo-Treading/api/proto/trade_execution v0.0.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.47
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.82.1
)

replace github.com/RohitIndira/Algo-Treading/api/proto/trade_execution => ../../api/proto/trade_execution

replace github.com/RohitIndira/Algo-Treading/api/proto/common => ../../api/proto/common

require (
	github.com/RohitIndira/Algo-Treading/api/proto/common v0.0.0 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
