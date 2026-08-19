package warc

import (
	"bytes"
	"testing"
)

// TestIterateFromOffsets checks that the reported byte span of each record is
// the span a range request would have to ask for. The spans have to be
// contiguous and cover the file exactly, because a gap means a fetch of the
// next record starts mid gzip member and fails.
func TestIterateFromOffsets(t *testing.T) {
	members := [][]byte{
		member(t, "response", "https://a.example/", "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<title>A</title>"),
		member(t, "response", "https://b.example/", "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<title>B</title>"),
		member(t, "response", "https://c.example/", "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<title>C</title>"),
	}
	var file bytes.Buffer
	for _, m := range members {
		file.Write(m)
	}

	var got []Record
	if err := IterateFrom(bytes.NewReader(file.Bytes()), 0, func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(members) {
		t.Fatalf("got %d records, want %d", len(got), len(members))
	}

	var want int64
	for i, r := range got {
		if r.Header.WARCOffset != want {
			t.Errorf("record %d offset = %d, want %d", i, r.Header.WARCOffset, want)
		}
		if r.Header.WARCLength != int64(len(members[i])) {
			t.Errorf("record %d length = %d, want %d", i, r.Header.WARCLength, len(members[i]))
		}
		want += int64(len(members[i]))
	}
	if want != int64(file.Len()) {
		t.Errorf("spans cover %d bytes, file is %d", want, file.Len())
	}
}

// TestIterateFromSpansAreFetchable takes the span each record reported and
// parses just those bytes, which is what fetch does with a range request.
func TestIterateFromSpansAreFetchable(t *testing.T) {
	var file bytes.Buffer
	file.Write(member(t, "response", "https://a.example/", "HTTP/1.1 200 OK\r\n\r\nA"))
	file.Write(member(t, "response", "https://b.example/", "HTTP/1.1 200 OK\r\n\r\nB"))
	raw := file.Bytes()

	var got []Record
	if err := IterateFrom(bytes.NewReader(raw), 0, func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for i, r := range got {
		slice := raw[r.Header.WARCOffset : r.Header.WARCOffset+r.Header.WARCLength]
		var one []Record
		if err := Iterate(bytes.NewReader(slice), func(rr Record) error {
			one = append(one, rr)
			return nil
		}); err != nil {
			t.Fatalf("record %d: reparse span: %v", i, err)
		}
		if len(one) != 1 {
			t.Fatalf("record %d: span holds %d records, want 1", i, len(one))
		}
		if one[0].Header.TargetURI != r.Header.TargetURI {
			t.Errorf("record %d: span holds %q, want %q", i, one[0].Header.TargetURI, r.Header.TargetURI)
		}
	}
}

// TestIterateFromBase checks the base offset a resumed read starts at is added
// to every span, so a stream that died at byte N and restarted there still
// reports positions in the whole file.
func TestIterateFromBase(t *testing.T) {
	first := member(t, "response", "https://a.example/", "HTTP/1.1 200 OK\r\n\r\nA")
	second := member(t, "response", "https://b.example/", "HTTP/1.1 200 OK\r\n\r\nB")
	base := int64(len(first))

	var got []Record
	if err := IterateFrom(bytes.NewReader(second), base, func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].Header.WARCOffset != base {
		t.Errorf("offset = %d, want %d", got[0].Header.WARCOffset, base)
	}
	if got[0].Header.WARCLength != int64(len(second)) {
		t.Errorf("length = %d, want %d", got[0].Header.WARCLength, len(second))
	}
}
