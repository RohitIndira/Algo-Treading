module github.com/RohitIndira/Algo-Treading/services/user-config

go 1.23.0

toolchain go1.24.6

require (
	github.com/RohitIndira/Algo-Treading v0.0.0
	github.com/RohitIndira/Algo-Treading/api/proto/common v0.0.0
	github.com/RohitIndira/Algo-Treading/api/proto/user_config v0.0.0-00010101000000-000000000000
	github.com/RohitIndira/Algo-Treading/pkg/logger v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.49
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.68.1
)

require (
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.19 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/RohitIndira/Algo-Treading => ../..

replace github.com/RohitIndira/Algo-Treading/api/proto/common => ../../api/proto/common

replace github.com/RohitIndira/Algo-Treading/api/proto/user_config => ../../api/proto/user_config

replace github.com/RohitIndira/Algo-Treading/pkg/logger => ../../pkg/logger
