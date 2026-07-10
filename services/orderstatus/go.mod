module github.com/RohitIndira/Algo-Treading/services/orderstatus

go 1.23.0

require (
	github.com/RohitIndira/Algo-Treading/pkg/indira v0.0.0
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.47
	go.uber.org/zap v1.27.0
)

replace github.com/RohitIndira/Algo-Treading/pkg/indira => ../../pkg/indira

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	go.uber.org/multierr v1.10.0 // indirect
)
