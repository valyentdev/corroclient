package corroclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

type event struct {
	EOQ     *EOQ    `json:"eoq"`
	Columns Columns `json:"columns"`
	Row     []any   `json:"row"`
	Change  []any   `json:"change"`
	Error   *string `json:"error"`
}

var ErrMissedChange = errors.New("corrosubs: missed change")
var ErrMaxRetryExceeded = errors.New("corrosubs: lost connection")
var ErrSubscriptionClosed = errors.New("corrosubs: subscription closed")
var ErrUnrecoverableSub = errors.New("corrosubs: unrecoverable subscription")

var ErrUnknownEvent = errors.New("corroclient: unknown event in subscription")

func (c *CorroClient) postSubscription(ctx context.Context, statement Statement, skipRows bool, from uint64) (*http.Response, error) {
	data, err := json.Marshal(statement)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/v1/subscriptions?skip_rows=%t", skipRows)
	if from != 0 {
		path += fmt.Sprintf("&from=%d", from)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.getURL("/v1/subscriptions"), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, errNotFound
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("corroclient-error: %s", string(body))
}

var errNotFound = errors.New("corroclient: subscription not found")

func (c *CorroClient) getSub(ctx context.Context, subscriptionId string, skipRows bool, from uint64) (*http.Response, error) {
	path := fmt.Sprintf("/v1/subscriptions/%s?skip_rows=%t", subscriptionId, skipRows)
	if from != 0 {
		path += fmt.Sprintf("&from=%d", from)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.getURL(path), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errNotFound
	}

	return nil, errors.New("corrosubs: Invalid status code")
}
