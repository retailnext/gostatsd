package tcplistener

import (
	"bytes"
	"testing"
)

func TestRingBuf(t *testing.T) {
	b := newRingBuf(10)
	n, err := b.Write([]byte("0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("Expected write n to be 10 was %d", n)
	}

	n, err = b.Write([]byte("928"))
	if err == nil {
		t.Fatal("Expected error when write exceeds buffer capacity but got none")
	}
	if n != 0 {
		t.Fatalf("Expected n to be 0 when write exceeds buffer capacity but was %d", n)
	}

	if b.HasLines() {
		t.Fatal("Expected no lines in ring buf")
	}

	b = newRingBuf(10)

	actions := []action{
		{
			w:  []byte("a"),
			ec: 9,
		},
		{
			w:  []byte("a\nbb\nc"),
			ec: 3,
		},
		{
			r:  []byte("aa\n"),
			ec: 6,
		},
		{
			r:  []byte("bb\n"),
			ec: 9,
		},
		{
			w:  []byte("ccccc\nddd"),
			ec: 0,
		},
		{
			r:  []byte("cccccc\n"),
			ec: 7,
		},
		{
			w:  []byte("ddd\nee\n"),
			ec: 0,
		},
		{
			r:  []byte("dddddd\n"),
			ec: 7,
		},
		{
			r:  []byte("ee\n"),
			ec: 10,
		},
	}

	for i, a := range actions {
		if len(a.w) > 0 {
			_, err := b.Write(a.w)
			if err != nil {
				t.Fatalf("%d: (%q): write error: %s", i, a.w, err)
			}
		} else if len(a.r) > 0 {
			line := b.ReadLine()
			if !bytes.Equal(line, a.r) {
				t.Fatalf("%d: read mismatch: got=%q expect=%q", i, line, a.r)
			}
		}
		if b.Cap() != a.ec {
			t.Fatalf("%d: cap mismatch: got=%d expect=%d", i, b.Cap(), a.ec)
		}
	}
}

type action struct {
	w  []byte // data to write
	r  []byte // data expected after read
	ec int    // expected capacity after action
}
