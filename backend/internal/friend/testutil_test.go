package friend

import "context"

// context0 is the shared background context for tests.
var context0 = context.Background()

func testHash(label string) []byte { return []byte("payload-hash:" + label) }
