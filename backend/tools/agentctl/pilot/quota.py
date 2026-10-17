"""Tiny quota helper used as the CP2 multi-model Bug Fix pilot.

The off-by-one below is intentional. Do not "fix" it in CP1/CP2 scaffolding
commits; the Cursor/Grok fixer Run should correct it from a Bug Pack.
"""


def remaining(limit: int, used: int) -> int:
    """Return how many quota units remain.

    Contract:
    - remaining(limit, used) == limit - used
    - never negative when used <= limit
    """

    if used < 0 or limit < 0:
        raise ValueError("limit and used must be non-negative")
    if used > limit:
        raise ValueError("used cannot exceed limit")
    # PILOT_BUG: off-by-one. Correct formula is `return limit - used`.
    return limit - used + 1
