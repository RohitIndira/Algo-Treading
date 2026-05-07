package indira

// WebSocketTokenResponse represents the response from the REST API to get a WebSocket token
type WebSocketTokenResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  []struct {
		OrderToken string `json:"orderToken"`
	} `json:"result"`
}

// WSConnectionRequest represents the initial payload sent via text after connecting to WebSocket
type WSConnectionRequest struct {
	UserId     string `json:"userId"`
	OrderToken string `json:"orderToken"`
}

// WSHeartbeat represents the heartbeat to send every 50 seconds
type WSHeartbeat struct {
	UserId    string `json:"userId"`
	Heartbeat string `json:"heartbeat"` // Typically "h"
}

// FlexInt is a JSON type that accepts both number and string representations.
// The broker WS sends some fields as int in one message and string in another.
type FlexInt string

func (f *FlexInt) UnmarshalJSON(data []byte) error {
	// Accept both "123" and 123
	s := string(data)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	*f = FlexInt(s)
	return nil
}

func (f FlexInt) String() string { return string(f) }

// WSOrderStatus represents the main live order status structure from the WebSocket.
// Many fields use FlexInt because the broker inconsistently sends them as
// either JSON numbers or JSON strings across different message types.
type WSOrderStatus struct {
	OrderSequenceNumber   int     `json:"OrderSequenceNumber"`
	ExpiryDate            string  `json:"ExpiryDate"`
	PartCode              string  `json:"PartCode"`
	InitiatedByUserId     string  `json:"InitiatedByUserId"`
	BuySell               string  `json:"Buy_Sell"` // Usually "1" for buy, "2" for sell depending on API
	Product               string  `json:"Product"`
	OrderNumber           string  `json:"OrderNumber"`
	ModifiedBy            string  `json:"ModifiedBy"`
	DQ                    FlexInt `json:"DQ"`
	OrderType             string  `json:"OrderType"`
	Remarks               string  `json:"Remarks"`
	DecimalLocator        FlexInt `json:"DecimalLocator"`
	RegularLot            int     `json:"RegularLot"`
	ModifiedByUserId      string  `json:"ModifiedByUserId"`
	SpreadFlag            int     `json:"SpreadFlag"`
	MessageType           string  `json:"MessageType"`
	OrderPrice            string  `json:"OrderPrice"`
	Misc                  string  `json:"Misc"`
	GTDOrderStatus        int     `json:"GTDOrderStatus"`
	OrderEntryTime        string  `json:"OrderEntryTime"`
	DQRemaining           FlexInt `json:"DQRemaining"`
	ProCli                string  `json:"ProCli"`
	TradedQTY             FlexInt `json:"TradedQTY"`
	MessageSequenceNumber FlexInt `json:"MessageSequenceNumber"`
	Exchange              FlexInt `json:"Exchange"`
	TradedPrice           string  `json:"TradedPrice"`
	PendingQty            FlexInt `json:"PendingQty"`
	OptionType            string  `json:"Option_Type"`
	AMOOrderID            string  `json:"AMOOrderID"`
	LegIndicator          string  `json:"LegIndicator"`
	Symbol                string  `json:"Symbol"`
	TriggerPrice          float64 `json:"TriggerPrice"`
	Reason                string  `json:"Reason"`
	LastModifiedTimeStamp string  `json:"LastModifiedTimeStamp"`
	OrderOriginalQty      int     `json:"OrderOriginalQty"`
	OrderStatus           string  `json:"OrderStatus"`
	InstrumentName        string  `json:"InstrumentName"`
	ExchangeAlgoID        string  `json:"ExchangeAlgoID"`
	UserID                FlexInt `json:"UserID"`
	Days                  FlexInt `json:"Days"`
	CliOrderNumber        int     `json:"CliOrderNumber"`
	ManagerID             FlexInt `json:"ManagerID"`
	MarketType            int     `json:"MarketType"`
	UniqueCode            string  `json:"UniqueCode"` // Usually maps to ordId
	SpreadPrice           float64 `json:"SpreadPrice"`
	StrikePrice           float64 `json:"StrikePrice"`
	InitiatedBy           string  `json:"InitiatedBy"`
	Series                string  `json:"Series"`
	UserRemarks           string  `json:"UserRemarks"`
	UCC                   string  `json:"UCC"`
	ExchangeAccountCode   string  `json:"ExchangeAccountCode"`
	ScripCode             FlexInt `json:"ScripCode"`
	OrderValidity         string  `json:"OrderValidity"`
	LastModifiedTime      string  `json:"LastModifiedTime"`
	CPID                  string  `json:"CP_ID"`
	ModifyBit             int     `json:"ModifyBit"`
	SLLimitPrice          float64 `json:"SLLimitPrice"`
	OrderTimeStamp        string  `json:"OrderTimeStamp"`
}
