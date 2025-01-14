package corroclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (c *CorroClient) UpdateSchema(ctx context.Context, stmts []Statement) (*ExecResult, error) {
	payload, err := json.Marshal(stmts)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, "POST", c.getURL("/v1/migrations"), bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	resp, err := c.request(request)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		bodyErr, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("corroclient: invalid status code: %d, body: %s", resp.StatusCode, string(bodyErr))
	}

	var execResult ExecResult
	if err := json.NewDecoder(resp.Body).Decode(&execResult); err != nil {
		return nil, err
	}

	return &execResult, nil
}
