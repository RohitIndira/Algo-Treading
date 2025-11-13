module github.com/RohitIndira/Algo-Treading/services/trade-execution

go 1.21

require (
	github.com/google/uuid v1.5.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/lib/pq v1.10.9
	github.com/rabbitmq/amqp091-go v1.9.0
	google.golang.org/grpc v1.60.1
	google.golang.org/protobuf v1.32.0 // indirect
)

require (
	github.com/RohitIndira/Algo-Treading/api/proto/common v0.0.0
	github.com/RohitIndira/Algo-Treading/api/proto/trade_execution v0.0.0
	github.com/RohitIndira/Algo-Treading/pkg/odin v0.0.0
	github.com/joho/godotenv v1.5.1
)

require (
	github.com/golang/protobuf v1.5.3 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231212172506-995d672761c0 // indirect
)

replace github.com/RohitIndira/Algo-Treading/api/proto/common => ../../api/proto/common

replace github.com/RohitIndira/Algo-Treading/api/proto/trade_execution => ../../api/proto/trade_execution

replace github.com/RohitIndira/Algo-Treading/pkg/odin => ../../pkg/odin

replace github.com/RohitIndira/Algo-Treading/pkg/logger => ../../pkg/logger
