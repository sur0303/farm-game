import { describe, expect, it, vi } from 'vitest';
import { HttpFriendRequestError, HttpGameFriendService, HttpShareLinkCreator } from '../friends/game-friend-service';

const cropCycleId = 'sid1_pK6aJIBpMU-PpnlrN1v-fHZrdkGG_fgNJOFHG70Bg4s';
const authorityFields = {
  relation_id: 'relation-1', relation_epoch: '1', action_mask: 11,
  opportunity_status: 'READY', opportunity_revision: '7',
  theft_eligible_targets: [{ plot_id: 4, crop_cycle_id: cropCycleId }],
  validated_at_ms: 1_700_000_000_000,
};
const pageFields = { roster_revision: '3', validated_at_ms: 1_700_000_000_000 };

describe('authenticated HTTP friend boundary', () => {
  it('accepts only the authoritative flat avatar_id and level fields', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        friends: [{ owner_id: '10002', display_name: 'Bob', avatar_id: 6, level: 9, ...authorityFields }],
        ...pageFields,
      }),
    }));

    await expect(new HttpGameFriendService('/api').list('10001')).resolves.toEqual([{
      ownerId: '10002',
      displayName: 'Bob',
      avatarId: 6,
      level: 9,
      relationId: 'relation-1',
      relationEpoch: 1n,
      actionMask: 11,
      opportunityStatus: 'READY',
      opportunityRevision: 7n,
      theftEligibleTargets: [{ plotId: 4, cropCycleId }],
      validatedAtMs: 1_700_000_000_000,
    }]);
  });

  it('fails closed for nested or malformed profile fields', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ friends: [{ owner_id: '10002', display_name: 'Bob', public_profile: { avatar_id: 6, level: 9 }, ...authorityFields }], ...pageFields }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ friends: [{ owner_id: '10002', display_name: 'Bob', avatar_id: '6', level: 9, ...authorityFields }], ...pageFields }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ friends: [{ owner_id: '10002', display_name: 'Bob', avatar_id: 6, level: 0, ...authorityFields }], ...pageFields }),
      });
    vi.stubGlobal('fetch', fetchMock);
    const friends = new HttpGameFriendService('/api');

    await expect(friends.list('10001')).rejects.toThrow(/avatar_id malformed/);
    await expect(friends.list('10001')).rejects.toThrow(/avatar_id malformed/);
    await expect(friends.list('10001')).rejects.toThrow(/level malformed/);
  });

  it('fails closed when exact theft targets are malformed or disagree with authority status', async () => {
    const otherCycleId = 'sid1_b5wzCfNHotiHpKRT4j50m8cG9Z81yurh409UIDCzGR0';
    const malformed = [
      { ...authorityFields, opportunity_status: 'UNKNOWN' },
      { ...authorityFields, theft_eligible_targets: [] },
      { ...authorityFields, action_mask: 8 },
      { ...authorityFields, theft_eligible_targets: [{ plot_id: 4, crop_cycle_id: 'cycle-four' }] },
      { ...authorityFields, theft_eligible_targets: [
        { plot_id: 4, crop_cycle_id: cropCycleId }, { plot_id: 4, crop_cycle_id: otherCycleId },
      ] },
      { ...authorityFields, theft_eligible_targets: [
        { plot_id: 4, crop_cycle_id: cropCycleId }, { plot_id: 5, crop_cycle_id: cropCycleId },
      ] },
    ];
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => ({
      ok: true,
      json: async () => ({
        friends: [{ owner_id: '10002', display_name: 'Bob', avatar_id: 6, level: 9, ...malformed.shift() }],
        ...pageFields,
      }),
    })));
    const service = new HttpGameFriendService('/api');
    for (let index = 0; index < 6; index += 1) {
      await expect(service.list('10001')).rejects.toThrow(/theft/);
    }
  });

  it('preserves the HTTP status for an expired session instead of hiding it as a generic error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      json: async () => ({ error: 'unauthenticated' }),
    }));
    await expect(new HttpGameFriendService('/api').list('10001')).rejects.toEqual(
      new HttpFriendRequestError(401, 'unauthenticated'),
    );
  });

  it('keeps paged UNKNOWN explicit and never enables interaction bits from it', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          friends: [{ owner_id: '10002', display_name: 'Bob', avatar_id: 6, level: 9,
            ...authorityFields, action_mask: 8, opportunity_status: 'UNKNOWN', opportunity_revision: '0', theft_eligible_targets: [] }],
          next_cursor: 'opaque-next', ...pageFields,
        }),
      })
      .mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'UNKNOWN' }) });
    vi.stubGlobal('fetch', fetchMock);
    const service = new HttpGameFriendService('/api');
    const page = await service.listPage('10001', 'opaque-cursor', 20);
    expect(page.nextCursor).toBe('opaque-next');
    expect(page.friends[0]).toMatchObject({ actionMask: 8, opportunityStatus: 'UNKNOWN' });
    await expect(service.nextOpportunity('10001')).resolves.toEqual({ status: 'UNKNOWN' });
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/v1/friends?cursor=opaque-cursor&page_size=20');
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/api/v1/friends/opportunity');
  });

  it('accepts only a READY actionable server-owned next opportunity', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => ({
      status: 'FOUND', friend: { owner_id: '10003', display_name: 'Carol', avatar_id: 2, level: 4, ...authorityFields },
    }) }));
    await expect(new HttpGameFriendService('/api').nextOpportunity('10001')).resolves.toMatchObject({
      status: 'FOUND', friend: { ownerId: '10003', actionMask: 11, opportunityStatus: 'READY' },
    });
  });

  it('never sends a client-asserted viewer or inviter id', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true, json: async () => ({ friends: [], ...pageFields }) })
      .mockResolvedValueOnce({ ok: true, json: async () => ({ target_owner_id: '10001', inviter_label: '玩家10001' }) })
      .mockResolvedValueOnce({ ok: true, json: async () => ({ removed: true }) })
      .mockResolvedValueOnce({ ok: true, json: async () => ({ share_token: 'opaque-token' }) });
    vi.stubGlobal('fetch', fetchMock);
    const friends = new HttpGameFriendService('/api');

    await friends.list('spoofed-viewer');
    await friends.accept('opaque-token', 'spoofed-viewer');
    await friends.revoke('spoofed-viewer', '10001');
    await new HttpShareLinkCreator('/api').create('spoofed-inviter', 'https://farm.example');

    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/v1/friends');
    expect(fetchMock.mock.calls[1]?.[1]?.body).toBe(JSON.stringify({ share_token: 'opaque-token' }));
    expect(JSON.parse(String(fetchMock.mock.calls[2]?.[1]?.body))).toMatchObject({
      friend_owner_id: '10001',
      operation_id: expect.any(String),
    });
    expect(fetchMock.mock.calls[3]?.[1]?.body).toBe(JSON.stringify({}));
    for (const [, init] of fetchMock.mock.calls) {
      expect(init).toMatchObject({ credentials: 'include' });
    }
  });
});
