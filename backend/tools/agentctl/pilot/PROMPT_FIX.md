# Pilot task: fix quota remaining off-by-one

You are fixing a synthetic agentctl pilot bug. Scope is only `tools/agentctl/pilot/**`.

## Goal

Make `pilot.quota.remaining(limit, used)` satisfy:

- `remaining(limit, used) == limit - used`
- `remaining(3, 3) == 0`

## Non-goals

- Do not modify `tools/agentctl/agentctl.py` or game code.
- Do not redesign the controller.
- Do not weaken or delete the acceptance test.

## Acceptance

```bash
python3 -m unittest discover -s tools/agentctl/pilot -p 'test_*.py' -v
```
