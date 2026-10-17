import { describe, expect, it } from 'vitest';
import { readShareToken } from '../share/share-url';

describe('share intent', () => {
  it('reads a short-lived share token without treating an owner id as authority', () => {
    expect(readShareToken('https://farm.example/game?share_token=opaque-share-token&owner_id=player-b')).toBe('opaque-share-token');
    expect(readShareToken('https://farm.example/game?owner_id=player-b')).toBeNull();
  });
});
