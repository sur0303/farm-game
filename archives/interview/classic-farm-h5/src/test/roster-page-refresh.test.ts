import { describe, expect, it } from 'vitest';
import { preservePagedRosterBaseline, refreshedRosterNextCursor } from '../friends/roster-page-refresh';

describe('paged roster first-page refresh', () => {
  it('preserves loaded later pages only for a silent refresh of the same roster revision', () => {
    expect(preservePagedRosterBaseline(true, true, 7n, 7n)).toBe(true);
    expect(preservePagedRosterBaseline(false, true, 7n, 7n)).toBe(false);
    expect(preservePagedRosterBaseline(true, false, 7n, 7n)).toBe(false);
    expect(preservePagedRosterBaseline(true, true, 0n, 0n)).toBe(false);
    expect(preservePagedRosterBaseline(true, true, 7n, 8n)).toBe(false);
  });

  it('does not rewind the append cursor when only page one is polled', () => {
    expect(refreshedRosterNextCursor(true, 'after-page-2', 'after-page-1')).toBe('after-page-2');
    expect(refreshedRosterNextCursor(true, '', 'after-page-1')).toBe('');
    expect(refreshedRosterNextCursor(false, 'stale', 'new-page-1')).toBe('new-page-1');
  });
});
