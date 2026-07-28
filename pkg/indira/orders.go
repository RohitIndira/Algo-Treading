package indira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// ============ Order Management Methods ============

// PlaceOrder places a new order
// auth parameter contains user-specific authentication from frontend
func (c *Client) PlaceOrder(ctx context.Context, auth *AuthContext, req *PlaceOrderRequest) (*PlaceOrderResponse, error) {
	if req.AlgoID == "" {
		req.AlgoID = c.algoID
	}

	resp, err := c.doRequest(ctx, auth, "POST", "/order-services/api/order/v1/place-order", req)
	if err != nil {
		return nil, fmt.Errorf("place order request failed: %w", err)
	}

	// Parse response
	var orderResp PlaceOrderResponse
	if err := json.Unmarshal(resp.Data, &orderResp); err != nil {
		// Try alternative response format
		var altResp map[string]interface{}
		if err2 := json.Unmarshal(resp.Data, &altResp); err2 == nil {
			// Check for orderId or ordId field
			if orderId, ok := altResp["orderId"].(string); ok {
				orderResp.OrderId = orderId
				orderResp.OrdId = orderId
			} else if ordId, ok := altResp["ordId"].(string); ok {
				orderResp.OrderId = ordId
				orderResp.OrdId = ordId
			}
			if msg, ok := altResp["message"].(string); ok {
				orderResp.Message = msg
			}
			if status, ok := altResp["status"].(string); ok {
				orderResp.Status = status
			}
		} else {
			return nil, fmt.Errorf("failed to parse order response: %w", err)
		}
	}

	// Check for Indira business errors (e.g. EG003 "Price Not in multiple of PriceTick").
	// These use infoID/infoMsg fields, not status/message.
	// infoID "0" means success — only treat non-zero infoID as a broker rejection.
	if resp.InfoID != "" && resp.InfoID != "0" {
		return nil, &BrokerBusinessError{Code: resp.InfoID, Message: resp.InfoMsg}
	}

	// Check generic error status
	if orderResp.Status == "error" {
		return &orderResp, fmt.Errorf("order placement failed: %s", orderResp.Message)
	}

	return &orderResp, nil
}

// ModifyOrder modifies an existing order
// auth parameter contains user-specific authentication from frontend
func (c *Client) ModifyOrder(ctx context.Context, auth *AuthContext, req *ModifyOrderRequest) error {
	resp, err := c.doRequest(ctx, auth, "POST", "/order-services/api/order/v1/modify-order", req)
	if err != nil {
		return fmt.Errorf("modify order request failed: %w", err)
	}

	// Check for Indira business errors (infoID "0" = success).
	if resp.InfoID != "" && resp.InfoID != "0" {
		return &BrokerBusinessError{Code: resp.InfoID, Message: resp.InfoMsg}
	}

	// Check response
	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err == nil {
		if status, ok := result["status"].(string); ok && status == "error" {
			if msg, ok := result["message"].(string); ok {
				return fmt.Errorf("modify order failed: %s", msg)
			}
			return fmt.Errorf("modify order failed")
		}
	}

	return nil
}

// CancelOrder cancels an order
// auth parameter contains user-specific authentication from frontend
func (c *Client) CancelOrder(ctx context.Context, auth *AuthContext, req *CancelOrderRequest) error {
	resp, err := c.doRequest(ctx, auth, "POST", "/order-services/api/order/v1/cancel-order", req)
	if err != nil {
		return fmt.Errorf("cancel order request failed: %w", err)
	}

	// Check for Indira business errors (infoID "0" = success).
	if resp.InfoID != "" && resp.InfoID != "0" {
		return &BrokerBusinessError{Code: resp.InfoID, Message: resp.InfoMsg}
	}

	// Check response
	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err == nil {
		if status, ok := result["status"].(string); ok && status == "error" {
			if msg, ok := result["message"].(string); ok {
				return fmt.Errorf("cancel order failed: %s", msg)
			}
			return fmt.Errorf("cancel order failed")
		}
	}

	return nil
}

// GetBrokerageCharges calculates brokerage charges for a potential order
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetBrokerageCharges(ctx context.Context, auth *AuthContext, req *BrokerageChargesRequest) (map[string]interface{}, error) {
	resp, err := c.doRequest(ctx, auth, "POST", "/order-services/api/order/v1/brokerage-charges", req)
	if err != nil {
		return nil, fmt.Errorf("brokerage charges request failed: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse brokerage response: %w", err)
	}

	return result, nil
}

// GetOrderBook retrieves the order book
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetOrderBook(ctx context.Context, auth *AuthContext) ([]OrderBook, error) {
	resp, err := c.doRequest(ctx, auth, "GET", "/portfolio-services/api/order/v1/order-book", nil)
	if err != nil {
		return nil, fmt.Errorf("order book request failed: %w", err)
	}

	var orders []OrderBook
	if err := json.Unmarshal(resp.Data, &orders); err != nil {
		// Try alternative format (data might be nested)
		var altResp map[string]interface{}
		if err2 := json.Unmarshal(resp.Data, &altResp); err2 == nil {
			if ordersData, ok := altResp["orders"]; ok {
				ordersBytes, _ := json.Marshal(ordersData)
				if err3 := json.Unmarshal(ordersBytes, &orders); err3 != nil {
					return nil, fmt.Errorf("failed to parse order book: %w", err)
				}
			} else {
				return nil, fmt.Errorf("failed to parse order book: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to parse order book: %w", err)
		}
	}

	return orders, nil
}

// GetOrderTrail retrieves the order trail/history for a specific order
// auth parameter contains user-specific authentication from frontend
func (c *Client) GetOrderTrail(ctx context.Context, auth *AuthContext, req *OrderTrailRequest) ([]map[string]interface{}, error) {
	resp, err := c.doRequest(ctx, auth, "POST", "/portfolio-services/api/order/v2/order-trail", req)
	if err != nil {
		return nil, fmt.Errorf("order trail request failed: %w", err)
	}

	var trail []map[string]interface{}
	if err := json.Unmarshal(resp.Data, &trail); err != nil {
		return nil, fmt.Errorf("failed to parse order trail: %w", err)
	}

	return trail, nil
}

// GetTradeBook retrieves the trade book
// auth parameter contains user-specific authentication from frontend
// orderIds parameter is optional - if empty, returns all trades
func (c *Client) GetTradeBook(ctx context.Context, auth *AuthContext, orderIds ...string) ([]TradeBook, error) {
	path := "/portfolio-services/api/order/v1/trade-book"
	if len(orderIds) > 0 && orderIds[0] != "" {
		// Broker order IDs can contain characters that aren't valid unescaped in a
		// URL query value (e.g. '>' in "NZVND00015>7"); QueryEscape each one so the
		// request line stays well-formed. An unescaped '>' produces a raw HTTP 400
		// from the servlet container before the request ever reaches Indira's API.
		escaped := make([]string, len(orderIds))
		for i, id := range orderIds {
			escaped[i] = url.QueryEscape(id)
		}
		path += "?orderIds=" + strings.Join(escaped, ",")
	}

	resp, err := c.doRequest(ctx, auth, "POST", path, nil)
	if err != nil {
		return nil, fmt.Errorf("trade book request failed: %w", err)
	}

	var trades []TradeBook
	if err := json.Unmarshal(resp.Data, &trades); err != nil {
		// Try alternative format
		var altResp map[string]interface{}
		if err2 := json.Unmarshal(resp.Data, &altResp); err2 == nil {
			if tradesData, ok := altResp["trades"]; ok {
				tradesBytes, _ := json.Marshal(tradesData)
				if err3 := json.Unmarshal(tradesBytes, &trades); err3 != nil {
					return nil, fmt.Errorf("failed to parse trade book: %w", err)
				}
			} else {
				return nil, fmt.Errorf("failed to parse trade book: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to parse trade book: %w", err)
		}
	}

	return trades, nil
}
