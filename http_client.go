package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type httpClientPool struct {
	clients []*httpClient
	counter uint64
}

type httpClient struct {
	client   *http.Client
	upstream string
}

func newHTTPClientPool(upstreams []string) *httpClientPool {
	clients := make([]*httpClient, len(upstreams))
	for i, u := range upstreams {
		clients[i] = newHTTPClient(u)
	}

	return &httpClientPool{
		clients: clients,
		counter: 0,
	}
}

// Round robin client load balancer
func (p *httpClientPool) next() *httpClient {
	offset := atomic.AddUint64(&p.counter, 1) % uint64(len(p.clients))
	return p.clients[offset]
}

func (p *httpClientPool) Get(url string) (*http.Response, error) {
	c := p.next()
	return c.Get(url)
}

func newHTTPClient(upstream string) *httpClient {
	return &httpClient{
		upstream: upstream,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        keepalive,
				MaxIdleConnsPerHost: keepalive,
				MaxConnsPerHost:     keepalive,
				IdleConnTimeout:     10 * time.Minute,
			},
			// Skip follow http
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (c *httpClient) Get(url string) (*http.Response, error) {
	fullpath := fmt.Sprintf("%s/%s", strings.TrimRight(c.upstream, "/"), strings.TrimLeft(url, "/"))
	return c.client.Get(fullpath)
}
