package dis

import (
	"io"
	"strconv"
)

type Cmd struct {
	line []string
}

func (c Cmd) ClientReply(m ReplyMode) Cmd {
	c.line = append(c.line, "client", "reply", string(m))
	return c
}
func (c Cmd) Get(key string) Cmd {
	c.line = append(c.line, "get", key)
	return c
}
func (c Cmd) Set(key, value string) Cmd {
	c.line = append(c.line, "set", key, value)
	return c
}
func (c Cmd) Ex(s int) Cmd {
	c.line = append(c.line, "ex", strconv.Itoa(s))
	return c
}
func (c Cmd) Bytes() []byte {
	s := "*" + strconv.Itoa(len(c.line)) + "\r\n"
	for _, line := range c.line {
		s += "$" + strconv.Itoa(len(line)) + "\r\n" + line + "\r\n"
	}
	return []byte(s)
}
func (c Cmd) WriteTo(w io.Writer) (n int64, err error) {
	n0, err := w.Write(c.Bytes())
	return int64(n0), err
}
