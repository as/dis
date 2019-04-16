package dis

import (
	"bufio"
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

type Client struct {
	cmd  chan Cmd
	conn net.Conn
	done chan bool
	err  error
	Config
	buf []byte // [BufferCap]byte
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

// Get is only used for testing right now. As such, it creates a fresh
// connection per call. Don't use it for anything other than writing tests
func (c *Client) Get(key string) string {
	conn, _ := net.Dial("tcp", c.Addr)
	if conn == nil {
		return ""
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(time.Second))
	conn.Write(Cmd{}.Get(key).Bytes())

	conn.SetReadDeadline(time.Now().Add(time.Second))
	sc := bufio.NewScanner(conn)
	sc.Scan()
	sc.Scan()
	return sc.Text()
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
	c.buf = make([]byte, 0, BufferCap)
	for {
		select {
		case <-tm.C:
			c.flush()
		case cmd := <-c.cmd:
			c.buf = append(c.buf, cmd.Bytes()...)
			if len(c.buf) > BufferHiWater || len(c.cmd) <= ChanLoWater {
				c.flush()
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) flush() {
	if len(c.buf) == 0 {
		return
	}
	var conn net.Conn
Redial:
	conn = c.dial()
	if conn == nil {
		log.Println("redis: flush: retry logic could not reconnect, try again")
		goto Redial
	}
	_, err := conn.Write(c.buf)
	if err != nil {
		c.conn = nil
		goto Redial
	}
	c.buf = c.buf[:0]
}
