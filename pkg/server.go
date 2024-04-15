package pkg

import "net"

type Server interface {
	Serve(l net.Listener) error
}
