module github.com/RohitIndira/Algo-Treading/services/trade-execution

go 1.23.0

require (
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/lib/pq v1.10.9
	github.com/rabbitmq/amqp091-go v1.9.0
	google.golang.org/grpc v1.68.1
	google.golang.org/protobuf v1.34.2 // indirect
)

require (
	github.com/RohitIndira/Algo-Treading/pkg/indira v0.0.0
	github.com/joho/godotenv v1.5.1
	github.com/segmentio/kafka-go v0.4.49
	go.mongodb.org/mongo-driver v1.17.6
	go.uber.org/zap v1.27.0
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/sync v0.12.0 // indirect
)

require (
	github.com/RohitIndira/Algo-Treading/api/proto/common v0.0.0
	github.com/RohitIndira/Algo-Treading/api/proto/trade_execution v0.0.0
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.19 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
)

replace github.com/RohitIndira/Algo-Treading/api/proto/common => ../../api/proto/common

replace github.com/RohitIndira/Algo-Treading/api/proto/trade_execution => ../../api/proto/trade_execution

replace github.com/RohitIndira/Algo-Treading/pkg/indira => ../../pkg/indira

replace github.com/RohitIndira/Algo-Treading/pkg/logger => ../../pkg/logger
