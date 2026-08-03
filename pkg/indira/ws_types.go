package indira

import "strconv"

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

func atoiFlex(f FlexInt) int { n, _ := strconv.Atoi(string(f)); return n }

// FilledQty returns the PER-TRADE executed quantity for THIS frame. The broker's
// EXECUTED trade message (MessageType=TRD_MSG, verified live 2026-08-03) carries
// the fill in TradeQty, NOT TradedQTY (which is 0 on the ORD_NRML ack). Prefer
// TradeQty, then QuantityTradedToday, then legacy TradedQTY. NEVER derive from
// OrderOriginalQty — that's the ORDER size and over-states a partial.
//
// Use this for the append-only per-event broker_events log (each TRD_MSG = one
// trade). For "is the whole ORDER filled?" comparisons use CumulativeFilledQty.
func (w *WSOrderStatus) FilledQty() int {
	if v := atoiFlex(w.TradeQty); v > 0 {
		return v
	}
	if v := atoiFlex(w.QuantityTradedToday); v > 0 {
		return v
	}
	return atoiFlex(w.TradedQTY)
}

// CumulativeFilledQty returns the TOTAL filled quantity for the order so far.
// Prefer QuantityTradedToday (the running total across trades); fall back to
// legacy TradedQTY. Deliberately does NOT use TradeQty — a single trade's qty
// would under-state a multi-trade fill and make the partial-fill check
// (filledQty < orderQty) fire a SPURIOUS topup on a completing trade.
//
// Use this for order state (order.FilledQuantity) and every "is the order
// complete?" comparison in the execution path.
func (w *WSOrderStatus) CumulativeFilledQty() int {
	if v := atoiFlex(w.QuantityTradedToday); v > 0 {
		return v
	}
	return atoiFlex(w.TradedQTY)
}

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
	TradeQty              FlexInt `json:"TradeQty"`            // per-trade fill qty on TRD_MSG frames (0/absent on ORD_NRML)
	QuantityTradedToday   FlexInt `json:"QuantityTradedToday"` // cumulative fill qty on TRD_MSG frames
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
