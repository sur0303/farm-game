package scaleplan

import core "farmgame/internal/scaleplan"

// MemoryRepository aliases the core-package protocol fake. Keeping the
// implementation in core prevents testkit/adapters from acquiring the private
// blocked-terminal owner capability.
type MemoryRepository = core.ScalePlanTestRepository

var NewMemoryRepository = core.NewMemoryRepository
