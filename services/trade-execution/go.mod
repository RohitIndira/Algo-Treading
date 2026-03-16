module github.com/RohitIndira/Algo-Treading/services/trade-execution

go 1.23.0

require (
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/lib/pq v1.10.9
	github.com/rabbitmq/amqp091-go v1.9.0
	google.golang.org/grpc v1.68.1
	google.golang.org/protobuf v1.36.8 // indirect
)

require (
	github.com/RohitIndira/Algo-Treading/pkg/indira v0.0.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/joho/godotenv v1.5.1
	github.com/segmentio/kafka-go v0.4.49
	go.uber.org/zap v1.27.0
)

require (
	github.com/RohitIndira/Algo-Treading v0.0.0-00010101000000-000000000000
	github.com/gorilla/websocket v1.5.3
	github.com/prometheus/client_golang v1.23.2
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
)

require (
	github.com/RohitIndira/Algo-Treading/api/proto/common v0.0.0
	github.com/RohitIndira/Algo-Treading/api/proto/trade_execution v0.0.0
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.19 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
)

replace github.com/RohitIndira/Algo-Treading/api/proto/common => ../../api/proto/common

replace github.com/RohitIndira/Algo-Treading/api/proto/trade_execution => ../../api/proto/trade_execution

replace github.com/RohitIndira/Algo-Treading/pkg/indira => ../../pkg/indira

replace github.com/RohitIndira/Algo-Treading/pkg/logger => ../../pkg/logger

replace github.com/RohitIndira/Algo-Treading => ../../
