// This package is used to interact with the corrosion API.
package corroclient

import "net/http"

type Config struct {
	URL    string
	Bearer string
}

type CorroClient struct {
	c      *http.Client
	url    string
	bearer string
}

func (c *CorroClient) getURL(path string) string {
	return c.url + path
}

func NewCorroClient(config Config) *CorroClient {
	client := &http.Client{}
	corroClient := &CorroClient{
		c:      client,
		url:    config.URL,
		bearer: config.Bearer,
	}

	return corroClient
}

func (c *CorroClient) request(req *http.Request) (*http.Response, error) {
	if c.bearer != "" {
		req.Header.Set("Authorization", c.bearer)
	}
	if req.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set("Accept", "application/json")

	return c.c.Do(req)
}
