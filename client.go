package dis

import (
	"errors"
	"log"
	"net"
	"time"
)

const (
	DisableServerReply = true
	RetryNum           = 15
	RetryBaseDelay     = time.Second

	FlushEvery    = time.Second + time.Second/7
	BufferCap     = 32768
	BufferHiWater = BufferCap / 2
	ChanCap       = 32768
	ChanLoWater   = 32

	DefaultTTL          = 300 * time.Second
	DefaultReadDeadline = time.Second / 2
)

type ReplyMode string

const (
	ReplyOn   ReplyMode = "ON"
	ReplyOff  ReplyMode = "OFF"
	ReplySkip ReplyMode = "SKIP"
)

var ErrFirstDial = errors.New("initial dial failed")

type Config struct {
	Addr         string
	ReadDeadline time.Duration
	TTL          int // seconds
	Dialer       interface {
		Dial(network, address string) (net.Conn, error)
	}
}

func (c Config) ensure() Config {
	_, _, err := net.SplitHostPort(c.Addr)
	if err != nil {
		c.Addr = net.JoinHostPort(c.Addr, "6379")
	}
	if c.TTL == 0 {
		c.TTL = int(DefaultTTL) / int(time.Second)
	}
	if c.ReadDeadline == 0 {
		c.ReadDeadline = DefaultReadDeadline
	}
	return c
}

type Client struct {
	Config

	cmd  chan Cmd
	done chan bool

	conn net.Conn
	err  error
}

func NewClient(conf Config) *Client {
	conf = conf.ensure()
	c := &Client{Config: conf}
	c.cmd = make(chan Cmd, ChanCap)
	c.done = make(chan bool)
	go c.run()
	if c.dial() == nil {
		c.err = ErrFirstDial
	}
	return c
}

func (c *Client) Set(key, value string, sec int) {
	select {
	case c.cmd <- Cmd{}.Set(key, value).Ex(sec):
		//	case <-time.After(time.Second):
		//		log.Println("redis: writer: channel at capacity (can not connect to redis)")
	}
}

func (c *Client) Err() error {
	return c.err
}

func (c *Config) Dial(network, address string) (net.Conn, error) {
	if c.Dialer == nil {
		return net.Dial(network, address)
	}
	return c.Dialer.Dial(network, address)
}
// TODO(as): remove log statements
func (c *Client) dial() net.Conn {
	if c.conn != nil {
		return c.conn
	}
	log.Println("redis: dial: new tcp connection")
	var err error
	for i := time.Duration(0); i < RetryNum; i++ {
		time.Sleep(RetryBaseDelay * i)
		if c.conn, err = c.Dial("tcp", c.Addr); err == nil {
			break
		}
		
log.Println("redis: dial:", err)
	}
	if err != nil {
		log.Println("redis: dial:", err)
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
