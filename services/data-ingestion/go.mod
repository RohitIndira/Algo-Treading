module github.com/RohitIndira/Algo-Treading/services/data-ingestion

go 1.23.0

toolchain go1.24.6

require (
	github.com/RohitIndira/Algo-Treading v0.0.0
	github.com/RohitIndira/Algo-Treading/pkg/logger v0.0.0
	github.com/joho/godotenv v1.5.1
	go.uber.org/zap v1.27.1
)

	require (
		github.com/klauspost/compress v1.17.4 // indirect
		github.com/pierrec/lz4/v4 v4.1.19 // indirect
		github.com/segmentio/kafka-go v0.4.49 // indirect
		go.uber.org/multierr v1.11.0 // indirect
	)

replace github.com/RohitIndira/Algo-Treading => ../..

replace github.com/RohitIndira/Algo-Treading/pkg/logger => ../../pkg/logger
