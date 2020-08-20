package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/prologic/bitcask"
)

var (
	httpPort  int
	keepalive int
	upstreams string
	dbPath    string
	logPath   string

	maxFileSize  int
	maxValueSize uint64
	maxKeySize   uint64

	allowedExt = map[string]bool{
		".jpg":  true,
		".png":  true,
		".gif":  true,
		".jpeg": true,
	}
)

func main() {
	flag.IntVar(&httpPort, "port", 8080, "HTTP binding port")
	flag.IntVar(&keepalive, "keepalive", 64, "Number of keepalive connection to upstream")
	flag.StringVar(&upstreams, "upstreams", "", "A comma-separated list of upstream servers. Need to include http protocol")
	flag.StringVar(&dbPath, "dbPath", "", "Path to bitcask data dir")
	flag.StringVar(&logPath, "log", "-", "Path to log file")

	flag.IntVar(&maxFileSize, "maxFileSize", 2*1<<30, "Bitcask max datafile size (bytes)")
	flag.Uint64Var(&maxValueSize, "maxValueSize", 10*1<<20, "Bitcask max value size (bytes)")
	flag.Uint64Var(&maxKeySize, "maxKeySize", 200, "Bitcask max key size (bytes)")

	flag.Parse()
	if dbPath == "" {
		panic("Require dbPath")
	}

	logOut, err := getLogOutput(logPath)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logOut)

	db, err := bitcask.Open(
		dbPath,
		bitcask.WithMaxDatafileSize(maxFileSize),
		bitcask.WithMaxValueSize(maxValueSize),
		bitcask.WithMaxKeySize(uint32(maxKeySize)),
	)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	client := newHTTPClientPool(strings.Split(upstreams, ","))
	e := echo.New()
	e.Logger.SetOutput(logOut)
	e.GET("/*", func(c echo.Context) error {
		key := c.Request().URL.EscapedPath()
		ext := filepath.Ext(key)
		if !allowedExt[strings.ToLower(ext)] {
			return c.NoContent(http.StatusNotFound)
		}

		// Get from cache
		value, err := db.Get([]byte(key))
		if err == nil && value != nil {
			c.Response().Header().Add("X-BC-Cache-Status", "HIT")
			http.ServeContent(c.Response().Writer, c.Request(), key, time.Time{}, bytes.NewReader(value))
			return nil
		}

		// Get from upstream
		resp, err := client.Get(key)
		if err != nil {
			log.Printf("Unable to request from upstream. ERR: %v\n", err)
			return c.NoContent(http.StatusServiceUnavailable)
		}
		defer resp.Body.Close()

		// Handle response from upstream
		if resp.StatusCode == 200 {
			content, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Unable to read response from upstream. ERR: %v\n", err)
				return c.NoContent(http.StatusInternalServerError)
			}
			if err := db.Put([]byte(key), content); err != nil {
				log.Printf("Unable to save content to disk. ERR: %v\n", err)
				return c.NoContent(http.StatusInternalServerError)
			}
			c.Response().Header().Add("X-BC-Cache-Status", "MISS")
			http.ServeContent(c.Response().Writer, c.Request(), key, time.Time{}, bytes.NewReader(content))
			return nil
		} else if resp.StatusCode == 301 || resp.StatusCode == 302 {
			return c.Redirect(resp.StatusCode, resp.Header.Get("location"))
		}

		return c.NoContent(resp.StatusCode)
	})

	addr := fmt.Sprintf(":%d", httpPort)
	go e.Start(addr)

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit
	log.Println("Received stop signal. Prepare for shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		log.Printf("Failed to stop HTTP server. ERR: %v\n", err)
	}
	if err := db.Sync(); err != nil {
		log.Printf("Failed to sync bitcask. Might have data loss. ERR: %v\n", err)
	}
}

func getLogOutput(p string) (io.Writer, error) {
	if p == "-" {
		return os.Stdout, nil
	}
	return os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
}
