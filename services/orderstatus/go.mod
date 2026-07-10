module github.com/RohitIndira/Algo-Treading/services/orderstatus

go 1.23.0

require (
	github.com/RohitIndira/Algo-Treading/api/proto/user_config v0.0.0
	github.com/RohitIndira/Algo-Treading/pkg/indira v0.0.0
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.47
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.68.1
)

replace github.com/RohitIndira/Algo-Treading/api/proto/user_config => ../../api/proto/user_config

replace github.com/RohitIndira/Algo-Treading/api/proto/common => ../../api/proto/common

replace github.com/RohitIndira/Algo-Treading/pkg/indira => ../../pkg/indira

require (
	github.com/RohitIndira/Algo-Treading/api/proto/common v0.0.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
