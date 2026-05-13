module github.com/RohitIndira/Algo-Treading/services/hft-engine

go 1.23.0

require (
	github.com/RohitIndira/Algo-Treading/api/proto/hft_engine v0.0.0
	github.com/RohitIndira/Algo-Treading/pkg/indira v0.0.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/gorilla/websocket v1.5.3
	github.com/joho/godotenv v1.5.1
	github.com/lib/pq v1.10.9
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.68.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/RohitIndira/Algo-Treading/api/proto/hft_engine => ../../api/proto/hft_engine

replace github.com/RohitIndira/Algo-Treading/pkg/indira => ../../pkg/indira
