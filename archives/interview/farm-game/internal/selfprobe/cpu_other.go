//go:build !unix

package selfprobe

import "time"

// processCPUTime has no portable source outside unix, and a guessed value
// would be worse than an absent one: the qualification gate reads CPU
// headroom, so a fabricated number could pass a run that should fail.
func processCPUTime() time.Duration { return 0 }
