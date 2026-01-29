module github.com/RohitIndira/Algo-Treading/services/paper-execution

go 1.23.0

toolchain go1.24.6

require (
	github.com/RohitIndira/Algo-Treading/pkg/database/redis v0.0.0
	github.com/google/uuid v1.6.0
	github.com/joho/godotenv v1.5.1
	github.com/segmentio/kafka-go v0.4.49
	go.uber.org/zap v1.27.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-redis/redis/v8 v8.11.5 // indirect
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.19 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)

replace github.com/RohitIndira/Algo-Treading => ../..

replace github.com/RohitIndira/Algo-Treading/pkg/database/redis => ../../pkg/database/redis

replace github.com/RohitIndira/Algo-Treading/pkg/kafka => ../../pkg/kafka

replace github.com/RohitIndira/Algo-Treading/pkg/logger => ../../pkg/logger
