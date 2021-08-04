package fluentbit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	DefaultHTTPRetryTimeout = 3 * time.Second
	DefaultHTTPRetryBackoff = 150 * time.Millisecond
)

// Client for Fluent Bit Monitoring HTTP API.
type Client struct {
	HTTPClient *http.Client
	BaseURL    string
}

// BuildInfo payload returned by GET /
type BuildInfo struct {
	FluentBit struct {
		Version string   `json:"version"`
		Edition string   `json:"edition"`
		Flags   []string `json:"flags"`
	} `json:"fluent-bit"`
}

// UpTime paylaod returned by GET /api/v1/uptime
type UpTime struct {
	UpTimeSec uint64 `json:"uptime_sec"`
	// UpTimeHr is the human readable representation of uptime.
	UpTimeHr string `json:"uptime_hr"`
}

// Metrics payload returned by GET /api/v1/metrics
// Maps keyed by metric name.
type Metrics struct {
	Input  map[string]MetricInput  `json:"input"`
	Output map[string]MetricOutput `json:"output"`
}

type MetricInput struct {
	Records uint64 `json:"records"`
	Bytes   uint64 `json:"bytes"`
}

type MetricOutput struct {
	ProcRecords   uint64 `json:"proc_records"`
	ProcBytes     uint64 `json:"proc_bytes"`
	Errors        uint64 `json:"errors"`
	Retries       uint64 `json:"retries"`
	RetriesFailed uint64 `json:"retries_failed"`
}

type PluginStorage struct {
	Status struct {
		Overlimit bool   `json:"overlimit"`
		MemSize   string `json:"mem_size"`
		MemLimit  string `json:"mem_limit"`
	} `json:"status"`

	Chunks struct {
		Total    uint64 `json:"total"`
		Up       uint64 `json:"up"`
		Down     uint64 `json:"down"`
		Busy     uint64 `json:"busy"`
		BusySize string `json:"busy_size"`
	} `json:"chunks"`
}

type StorageMetrics struct {
	StorageLayer struct {
		Chunks struct {
			TotalChunks  uint64 `json:"total_chunks"`
			MemChunks    uint64 `json:"mem_chunks"`
			FsChunks     uint64 `json:"fs_chunks"`
			FsChunksUp   uint64 `json:"fs_chunks_up"`
			FsChunksDown uint64 `json:"fs_chunks_down"`
		} `json:"chunks"`
	} `json:"storage_layer"`

	InputChunks map[string]PluginStorage `json:"input_chunks"`
}

func (c *Client) BuildInfo(ctx context.Context) (BuildInfo, error) {
	var info BuildInfo
	return info, c.fetchJSON(ctx, "/", &info)
}

func (c *Client) UpTime(ctx context.Context) (UpTime, error) {
	var up UpTime
	return up, c.fetchJSON(ctx, "/api/v1/uptime", &up)
}

func (c *Client) Metrics(ctx context.Context) (Metrics, error) {
	var mm Metrics
	return mm, c.fetchJSON(ctx, "/api/v1/metrics", &mm)
}

func (c *Client) StorageMetrics(ctx context.Context) (StorageMetrics, error) {
	var mm StorageMetrics
	ctxWithTimeout, cancel := context.WithTimeout(ctx, DefaultHTTPRetryTimeout)
	defer cancel()
	return mm, c.fetchJSON(ctxWithTimeout, "/api/v1/storage", &mm)
}

func (c *Client) fetchJSON(ctx context.Context, endpoint string, ptr interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+endpoint, nil)
	if err != nil {
		return fmt.Errorf("could not create request: %w", err)
	}
	var resp *http.Response
	ticker := time.NewTicker(DefaultHTTPRetryBackoff)

loop:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout while trying to reach: %s", endpoint)
		case <-ticker.C:
			resp, err = c.HTTPClient.Do(req)
			if err == nil && resp.StatusCode != http.StatusNotFound {
				break loop
			}
		}
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("failed with status code %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(ptr)
	if err != nil {
		return fmt.Errorf("could not json unmarshal response: %w", err)
	}

	return nil
}
