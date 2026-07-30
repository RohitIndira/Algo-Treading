module github.com/RohitIndira/Algo-Treading/services/user-config

go 1.25.0

require (
	github.com/RohitIndira/Algo-Treading v0.0.0
	github.com/RohitIndira/Algo-Treading/api/proto/common v0.0.0
	github.com/RohitIndira/Algo-Treading/api/proto/user_config v0.0.0-00010101000000-000000000000
	github.com/RohitIndira/Algo-Treading/pkg/logger v0.0.0-00010101000000-000000000000
	github.com/go-redis/redis/v8 v8.11.5
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/joho/godotenv v1.5.1
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.49
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.82.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.19 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/RohitIndira/Algo-Treading => ../..

replace github.com/RohitIndira/Algo-Treading/pkg/logger => ../../pkg/logger

replace github.com/RohitIndira/Algo-Treading/api/proto/common => ../../api/proto/common

replace github.com/RohitIndira/Algo-Treading/api/proto/user_config => ../../api/proto/user_config
