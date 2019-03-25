package dis

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	c := NewClient(Config{"localhost:6379", time.Second})
	if c.Err() == ErrFirstDial {
		t.Skip("no redis server yet (and nothing running on 6379)")
		return
	}
	const alpha = "abcdefghijklmnopqrstuvwxyz"
	for _, i := range alpha {
		c.Set(string(i), "foo", c.TTL)
	}

	// fmt.Fprintf(conn, "%s", Cmd{}.ClientReply(ReplyOff).Bytes())
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	for _, i := range alpha {
		fmt.Fprintf(conn, "%s", Cmd{}.Get(string(i)).Bytes())
	}
	sc := bufio.NewScanner(conn)
	for range alpha {
		sc.Scan()
		t.Log(sc.Text())
	}
	conn.Close()
}

func TestSetEx(t *testing.T) {
	k, v := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/s/BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
	have := string(Cmd{}.Set(k, v).Ex(300).Bytes())
	want := golden
	if have != want {
		t.Fatalf("have %q want %q", have, want)
	}
}

const golden = "*5\r\n$3\r\nset\r\n$75\r\nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/s/BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\r\n$36\r\nCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\r\n$2\r\nex\r\n$3\r\n300\r\n"
