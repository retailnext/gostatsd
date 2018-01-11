package riemann

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/backends/riemann/riemannpb"
	"github.com/atlassian/gostatsd/pkg/backends/sender"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	BackendName         = "riemann"
	DefaultAddress      = "localhost:5555"
	DefaultDialTimeout  = 5 * time.Second
	DefaultWriteTimeout = 30 * time.Second
	DefaultNetwork      = "tcp"

	// sendChannelSize specifies the size of the buffer of a channel between caller goroutine, producing buffers, and the
	// goroutine that writes them to the socket.
	sendChannelSize = 1000
	// maxConcurrentSends is the number of max concurrent SendMetricsAsync calls that can actually make progress.
	maxConcurrentSends = 10
)

type Client struct {
	addr        string
	sender      sender.Sender
	dialTimeout time.Duration
}

func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	rv := getSubViper(v, "riemann")
	rv.SetDefault("address", DefaultAddress)
	rv.SetDefault("dial_timeout", DefaultDialTimeout)
	rv.SetDefault("write_timeout", DefaultWriteTimeout)
	rv.SetDefault("network", DefaultNetwork)
	return NewClient(
		rv.GetString("address"),
		rv.GetString("network"),
		rv.GetDuration("dial_timeout"),
		rv.GetDuration("write_timeout"),
	)
}

func NewClient(addr, network string, dialTimeout, writeTimeout time.Duration) (*Client, error) {
	switch network {
	case "tcp":
	default:
		return nil, fmt.Errorf(`unsupported "network" value`)
	}

	client := &Client{
		addr:        addr,
		dialTimeout: dialTimeout,
	}

	client.sender = sender.Sender{
		ConnFactory: client.connFactory,
		Sink:        make(chan sender.Stream, maxConcurrentSends),
		BufPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		WriteTimeout: writeTimeout,
	}

	return client, nil
}

func (client *Client) Run(ctx context.Context) {
	client.sender.Run(ctx)
}

func (c *Client) connFactory() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
	if err != nil {
		return nil, err
	}

	// riemann sends back response messages which we need to
	// consume
	go c.handleRiemannResponses(conn)
	return conn, nil
}

func (c *Client) Name() string {
	return BackendName
}

// SendMetricsAsync flushes the metrics to the riemann server, preparing payload synchronously but doing the send asynchronously.
func (c *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	sink := make(chan *bytes.Buffer, sendChannelSize)
	defer close(sink)

	select {
	case <-ctx.Done():
		cb([]error{ctx.Err()})
		return
	case c.sender.Sink <- sender.Stream{Ctx: ctx, Cb: cb, Buf: sink}:
	}

	events := make([]*riemannpb.Event, 0, 100)

	flush := func() (ok bool) {
		if len(events) == 0 {
			return true
		}
		msg := riemannpb.Msg{
			Events: events,
		}
		rawMsg, err := proto.Marshal(&msg)
		if err != nil {
			log.Warnf("[%s] proto marshal error: %s", BackendName, err)
			return true
		}
		buf := make([]byte, len(rawMsg)+4)
		binary.BigEndian.PutUint32(buf, uint32(len(rawMsg)))

		copy(buf[4:], rawMsg)

		events = events[:0]
		select {
		case <-ctx.Done():
			return false
		case sink <- bytes.NewBuffer(buf):
			return true
		}
	}

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		events = append(events, &riemannpb.Event{
			Service:     key,
			State:       "ok",
			Description: key,
			Tags:        counter.Tags,
			MetricD:     float64(counter.Value),
			Host:        counter.Hostname,
		})
		if len(events) >= 100 {
			if ok := flush(); !ok {
				return
			}
		}
	})
	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		for _, tr := range timer.Values {
			events = append(events, &riemannpb.Event{
				Service:     key,
				State:       "ok",
				Description: key,
				Tags:        timer.Tags,
				MetricD:     tr,
				Host:        timer.Hostname,
			})
			if len(events) >= 100 {
				if ok := flush(); !ok {
					return
				}
			}
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		events = append(events, &riemannpb.Event{
			Service:     key,
			State:       "ok",
			Description: key,
			Tags:        gauge.Tags,
			MetricD:     gauge.Value,
			Host:        gauge.Hostname,
		})
		if len(events) >= 100 {
			if ok := flush(); !ok {
				return
			}
		}
	})

	flush()
}

func (client *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// Read and discard response messages from riemann
func (c *Client) handleRiemannResponses(conn net.Conn) {
	io.Copy(ioutil.Discard, conn)
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
