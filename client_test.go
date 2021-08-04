package fluentbit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	semver "github.com/hashicorp/go-version"
	"github.com/ory/dockertest/v3"
)

var baseURL string

var defaultTestConfig = `
[SERVICE]
     HTTP_Server On
     HTTP_Listen 0.0.0.0
     HTTP_Port 2020
     storage.metrics On
[INPUT]
     name cpu
[OUTPUT]
     name stdout
`

const (
	version             = "1.8"
	flushInterval       = 1
	fluentBitConfigName = "fluent-bit.conf"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	pool, err := dockertest.NewPool("")
	if err != nil {
		fmt.Printf("could not create docker pool: %v\n", err)
		return 1
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Printf("cannot get cwd: %s", err.Error())
		return 1
	}

	// docker only support mounting volumes from cwd.
	configTmpFile, err := ioutil.TempFile(cwd, fluentBitConfigName)
	if err != nil {
		fmt.Printf("cannot create temp configuration file: %s", err.Error())
		return 1
	}

	_, err = configTmpFile.Write([]byte(defaultTestConfig))
	if err != nil {
		fmt.Printf("cannot write temp configuration file: %s", err.Error())
		return 1
	}

	fluentBitContainer, err := setupFluentBitContainer(pool, configTmpFile.Name())
	if err != nil {
		fmt.Printf("could not setup fluent bit container: %v\n", err)
		return 1
	}

	defer func() {
		err := os.Remove(configTmpFile.Name())
		if err != nil {
			fmt.Printf("can't remove temp config file: %s", err.Error())
		}

		err = pool.Purge(fluentBitContainer)
		if err != nil {
			fmt.Printf("could not cleanup fluentbit container: %v\n", err)
		}
	}()

	baseURL, err = getFluentBitContainerBaseURL(pool, fluentBitContainer)
	if err != nil {
		fmt.Printf("could not get fluent bit container base URL: %v\n", err)
		return 1
	}

	return m.Run()
}

func setupFluentBitContainer(pool *dockertest.Pool, configPath string) (*dockertest.Resource, error) {
	return pool.RunWithOptions(&dockertest.RunOptions{
		Mounts:     []string{configPath + ":/" + fluentBitConfigName},
		Repository: "fluent/fluent-bit",
		Tag:        version,
		Cmd:        []string{"/fluent-bit/bin/fluent-bit", "-c", "/" + fluentBitConfigName},
	})
}

func getFluentBitContainerBaseURL(pool *dockertest.Pool, container *dockertest.Resource) (string, error) {
	var baseURL string
	err := pool.Retry(func() error {
		hostPort := container.GetHostPort("2020/tcp")
		if hostPort == "" {
			return errors.New("empty fluentbit container host-port for port 2020")
		}
		baseURL = "http://" + hostPort
		ok, err := ping(baseURL)
		if err != nil {
			return fmt.Errorf("could not ping %q: %w", baseURL, err)
		}

		if !ok {
			return fmt.Errorf("%q not ready yet", baseURL)
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	time.Sleep(time.Second * flushInterval) // to be surer the server is ready.

	return baseURL, nil
}

func ping(u string) (bool, error) {
	resp, err := http.DefaultClient.Get(u)
	if err != nil {
		return false, fmt.Errorf("could not do request: %w", err)
	}

	defer resp.Body.Close()

	ok := resp.StatusCode < http.StatusBadRequest
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return false, fmt.Errorf("could not discard response body: %w", err)
	}

	return ok, nil
}

func TestClient_BuildInfo(t *testing.T) {
	client := &Client{
		HTTPClient: http.DefaultClient,
		BaseURL:    baseURL,
	}

	ctx := context.Background()
	info, err := client.BuildInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		constraint, err := semver.NewConstraint(fmt.Sprintf(">= %s", version))
		if err != nil {
			t.Fatal(err)
		}

		got, err := semver.NewSemver(info.FluentBit.Version)
		if err != nil {
			t.Fatal(err)
		}

		if !constraint.Check(got) {
			t.Fatalf("expected version to be %s; got %q", constraint, got)
		}
	}

	if want, got := "Community", info.FluentBit.Edition; want != got {
		t.Fatalf("expected edition to be %q; got %q", want, got)
	}
}

func TestClient_UpTime(t *testing.T) {
	client := &Client{
		HTTPClient: http.DefaultClient,
		BaseURL:    baseURL,
	}

	ctx := context.Background()
	up, err := client.UpTime(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want := time.Second
	time.Sleep(want)

	if got := time.Second * time.Duration(up.UpTimeSec); !(got >= want) {
		t.Fatalf("expected uptime to be >= %s; got %q", want, got)
	}
}

func TestClient_Metrics(t *testing.T) {
	client := &Client{
		HTTPClient: http.DefaultClient,
		BaseURL:    baseURL,
	}

	ctx := context.Background()
	mm, err := client.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if want, got := 1, len(mm.Input); got < want {
		fmt.Println(got >= want)
		t.Fatalf("expected inputs len to be >= %d; got %d", want, got)
	}

	if want, got := 1, len(mm.Output); got < want {
		t.Fatalf("expected outputs len to be >= %d; got %d", want, got)
	}
}

func TestClient_StorageMetrics(t *testing.T) {
	client := &Client{
		HTTPClient: http.DefaultClient,
		BaseURL:    baseURL,
	}

	mm, err := client.StorageMetrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if want, got := 1, mm.StorageLayer.Chunks.TotalChunks; int(got) < want {
		t.Fatalf("expected storage total chunks to be >= %d; got %d", want, got)
	}

	if want, got := 1, len(mm.InputChunks); got < want {
		t.Fatalf("expected input chunks len to be >= %d; got %d", want, got)
	}
}
