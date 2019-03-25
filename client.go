package dis

import (
	"errors"
	"net"
	"time"
)

const (
	DisableServerReply = true

	RetryNum       = 3
	RetryBaseDelay = time.Second

	FlushEvery    = time.Second
	BufferCap     = 32678
	BufferHiWater = BufferCap / 2
	ChanCap       = 16
	ChanLoWater   = ChanCap / 5
)

type ReplyMode string

const (
	ReplyOn   ReplyMode = "ON"
	ReplyOff  ReplyMode = "OFF"
	ReplySkip ReplyMode = "SKIP"
)

var ErrFirstDial = errors.New("initial dial failed")

type Config struct {
	Addr string
	TTL  time.Duration
}

type Client struct {
	Config

	cmd  chan *Cmd
	done chan bool

	conn net.Conn
	err  error
}

func NewClient(conf Config) *Client {
	c := &Client{Config: conf}
	c.cmd = make(chan *Cmd, 1)
	c.done = make(chan bool)
	go c.run()
	if c.dial() == nil {
		c.err = ErrFirstDial
	}
	return c
}

func (c *Client) Set(key, value string, ex time.Duration) {
	c.cmd <- Cmd{}.Set(key, value).Ex(int(ex / time.Second))
}

func (c *Client) Err() error {
	return c.err
}

func (c *Client) dial() net.Conn {
	var err error
	if c.conn != nil && err == nil {
		return c.conn
	}
	for i := time.Duration(0); i < RetryNum; i++ {
		time.Sleep(RetryBaseDelay * i)
		if c.conn, err = net.Dial("tcp", c.Addr); err == nil {
			break
		}
	}
	if err != nil {
		return nil
	}
	if DisableServerReply {
		c.conn.Write(Cmd{}.ClientReply(ReplyOff).Bytes())
	}
	return c.conn
}

func (c *Client) run() {
	tm := time.NewTimer(FlushEvery)
	buf := make([]byte, 0, BufferHiWater)
	flush := func() {
		if len(buf) > 0 {
			if conn := c.dial(); conn != nil {
				c.dial().Write(buf)
				buf = buf[:0]
			}
		}
	}
	for {
		select {
		default:
		case <-c.done:
			return
		}
		select {
		case <-tm.C:
			flush()
		case cmd := <-c.cmd:
			buf = append(buf, cmd.Bytes()...)
			if len(c.cmd) <= ChanLoWater {
				flush()
			}
		case <-c.done:
			return
		}
	}
}
