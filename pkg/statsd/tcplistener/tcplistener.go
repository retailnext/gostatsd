package tcplistener

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

func SocketFactory(addr string) func() (net.PacketConn, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// bubble up our listen error when the factory gets invoved
		return func() (net.PacketConn, error) {
			return nil, err
		}
	}

	return func() (net.PacketConn, error) {
		pc := &tcpPacketConn{
			ln:         ln,
			packetChan: make(chan packet),
		}

		ctx := context.Background()

		go pc.acceptLoop(ctx)
		return pc, nil
	}
}

// tcpPacketConn wraps a tcp connection and provides
// a net.PacketConn interface.
// tcpPacketConn will split the tcp stream into
// datagrams at newlines. This only works if the inner
// protocol is actually the statsd protocol.
type tcpPacketConn struct {
	ln         net.Listener
	packetChan chan packet
	err        error
}

func (pc *tcpPacketConn) acceptLoop(ctx context.Context) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	for {
		conn, err := pc.ln.Accept()
		if err != nil {
			pc.err = err
			return
		}

		go pc.readLoop(ctx, conn)
	}
}

type packet struct {
	addr net.Addr
	data []byte
}

type ringBuf struct {
	data         []byte
	firstIdx     int
	inUseBytes   int
	newlineCount int
}

func newRingBuf(size int) *ringBuf {
	return &ringBuf{
		data: make([]byte, size),
	}
}

func (r *ringBuf) Write(b []byte) (int, error) {
	if r.Cap() < len(b) {
		return 0, fmt.Errorf("Write %d exceeds ringBuf capacity %d", len(b), r.Cap())
	}

	nextFreeSlot := (r.firstIdx + r.inUseBytes) % len(r.data)
	if nextFreeSlot+len(b) < len(r.data) {
		copy(r.data[nextFreeSlot:], b)
	} else {
		firstLen := len(r.data) - nextFreeSlot
		first := b[:firstLen]
		copy(r.data[nextFreeSlot:], first)
		last := b[firstLen:]
		copy(r.data, last)
	}

	for _, c := range b {
		if c == '\n' {
			r.newlineCount++
		}
	}

	r.inUseBytes = r.inUseBytes + len(b)
	return len(b), nil
}

func (b *ringBuf) Cap() int {
	return len(b.data) - b.inUseBytes
}

func (b *ringBuf) HasLines() bool {
	return b.newlineCount > 0
}

func (b *ringBuf) ReadLine() []byte {
	if b.newlineCount < 1 {
		return nil
	}

	lineLen := 0
	for i := b.firstIdx; i < b.firstIdx+b.inUseBytes; i++ {
		lineLen++
		idx := i % len(b.data)
		if b.data[idx] == '\n' {
			break
		}
	}
	out := make([]byte, lineLen)
	if b.firstIdx+lineLen < len(b.data) {
		copy(out, b.data[b.firstIdx:b.firstIdx+lineLen])
		b.firstIdx = b.firstIdx + lineLen
	} else {
		firstPartLen := len(b.data) - b.firstIdx
		copy(out, b.data[b.firstIdx:])
		remaining := lineLen - firstPartLen
		copy(out[firstPartLen:], b.data[:remaining])
		b.firstIdx = remaining
	}
	b.inUseBytes -= lineLen
	b.newlineCount--
	return out
}

func (pc *tcpPacketConn) readLoop(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	readBuf := make([]byte, 1500) // used for network reads
	ringBuf := newRingBuf(1500)   // used to assemble full lines

	pkt := packet{
		addr: conn.RemoteAddr(),
		data: make([]byte, 0, 1500),
	}

	for done := false; !done; {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	INNERLOOP:
		for {
			if ringBuf.Cap() < 1 {
				break INNERLOOP
			}

			n, readErr := conn.Read(readBuf[:ringBuf.Cap()])
			if n > 0 {
				_, writeErr := ringBuf.Write(readBuf[:n])
				if writeErr != nil {
					log.Warnf("[tcplistener] ringbuf write error: err=%s addr=%s", writeErr, conn.RemoteAddr())
					return
				}
			}

			if readErr != nil {
				if readErr == io.EOF {
					done = true
					break INNERLOOP
				}
				if nerr, ok := readErr.(net.Error); ok {
					if nerr.Timeout() {
						break INNERLOOP
					}
				} else {
					log.Warnf("[tcplistener] read error err=%s addr=%s", readErr, conn.RemoteAddr())
				}
			}
		}

		if !ringBuf.HasLines() && ringBuf.Cap() < 1 {
			// payload too big
			// kill the connection
			log.Warnf("[tcplistener] message larger than buffer, closing: addr=%s buf=%v", conn.RemoteAddr(), ringBuf.data)
			return
		}

		if ringBuf.HasLines() {
			// its safe to reuse this buffer because we do a copy in ReadFrom and packetChan is unbuffered
			pkt.data = pkt.data[:0]
			for ringBuf.HasLines() {
				pkt.data = append(pkt.data, ringBuf.ReadLine()...)
			}

			select {
			case <-ctx.Done():
				return
			case pc.packetChan <- pkt:
			}
		}
	}
}

func (pc *tcpPacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	pkt, ok := <-pc.packetChan
	if !ok {
		return 0, nil, io.EOF
	}
	copy(b, pkt.data)
	return len(pkt.data), pkt.addr, nil
}

func (pc *tcpPacketConn) Close() error {
	close(pc.packetChan)
	return pc.ln.Close()
}

var unsupportedCallErr = errors.New("Not supported")

func (pc *tcpPacketConn) WriteTo(b []byte, addr net.Addr) (n int, err error) {
	return 0, unsupportedCallErr
}

// LocalAddr returns the local network address.
func (pc *tcpPacketConn) LocalAddr() net.Addr {
	return pc.ln.Addr()
}

func (pc *tcpPacketConn) SetDeadline(t time.Time) error {
	return unsupportedCallErr
}

func (pc *tcpPacketConn) SetReadDeadline(t time.Time) error {
	return unsupportedCallErr
}

func (pc *tcpPacketConn) SetWriteDeadline(t time.Time) error {
	return unsupportedCallErr
}
