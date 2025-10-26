package fakertest

import (
	"testing"

	rand "math/rand"

	faker "github.com/go-faker/faker/v4"
)

// A deterministic crypto.Reader: always returns 0x00.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

/*
--- Move these two init() blocks above/below each other to flip the test ---

If zeroReader's init() is LAST → faker's UUID will be:

	00000000-0000-4000-8000-000000000000

If cryptorand.Reader init() is LAST → the UUID will be random and the test fails.
*/

// func init() { // option A: deterministic, proves order-dependence
// 	faker.SetCryptoSource(zeroReader{})
// }

func Test_A(t *testing.T) {
	faker.SetCryptoSource(rand.New(rand.NewSource(1234)))
	const expected = "c00e5d67-c275-431c-89ad-ed7d8b151c57" // v4+variant bits over zeroes
	got := faker.UUIDHyphenated()
	if got != expected {
		t.Fatalf("expected %q when zeroReader init runs last; got %q", expected, got)
	}
}

func Test_B(t *testing.T) {
	faker.SetCryptoSource(rand.New(rand.NewSource(1337)))
	const expected = "26c5a418-2a81-4a50-82f5-45cbc6b1cd2b" // v4+variant bits over zeroes
	got := faker.UUIDHyphenated()
	if got != expected {
		t.Fatalf("expected %q when zeroReader init runs last; got %q", expected, got)
	}
}
