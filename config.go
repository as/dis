package dis

// Config configures the client
type Config struct {
	Addr         string
	ReadDeadline time.Duration
	TTL          int // seconds
	Dialer       interface {
		Dial(network, address string) (net.Conn, error)
	}
}

// ensure ensures that the config has reasonable defaults
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

// Dial dials the network and address using the config-provided dialer
// or with net.Dialer if that config dialer is nil-valued.
func (c *Config) Dial(network, address string) (net.Conn, error) {
	if c.Dialer == nil {
		return net.Dial(network, address)
	}
	return c.Dialer.Dial(network, address)
}
