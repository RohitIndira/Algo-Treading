module github.com/RohitIndira/Algo-Treading/services/indira-wrapper

go 1.24.0

require (
	github.com/RohitIndira/Algo-Treading/api/proto/indira_wrapper v0.0.0
	github.com/RohitIndira/Algo-Treading/pkg/indira v0.0.0
	github.com/gorilla/websocket v1.5.3
	google.golang.org/grpc v1.68.1
)

require (
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/RohitIndira/Algo-Treading/api/proto/indira_wrapper => ../../api/proto/indira_wrapper

replace github.com/RohitIndira/Algo-Treading/pkg/indira => ../../pkg/indira
