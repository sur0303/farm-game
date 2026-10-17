import type { ShareIntentResolver, ShareLinkCreator, SharePreview, ShareResolution } from '../share/share-intent';
import { FriendActionMask, type FriendRosterPage, type FriendRosterProvider, type FriendSummary } from './friend-roster';
import { createUuid } from '../platform/random-id';
import { validateStableOpaqueId } from '../protocol/work-identity';

export interface GameFriendService extends FriendRosterProvider, ShareIntentResolver {
  revoke(viewerId: string, friendOwnerId: string): Promise<void>;
  hasFriendship(viewerId: string, friendOwnerId: string): boolean;
  nextOpportunity(viewerId: string): Promise<FriendOpportunity>;
}

export type FriendOpportunity =
  | { status: 'FOUND'; friend: FriendSummary }
  | { status: 'UNKNOWN' }
  | { status: 'NONE' };

const relationshipKey = (left: string, right: string): string => [left, right].sort().join('::');

export class HttpFriendRequestError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
    this.name = 'HttpFriendRequestError';
  }
}

const jsonRequest = async <T>(url: string, init?: RequestInit): Promise<T> => {
  const response = await fetch(url, {
    credentials: 'include',
    cache: 'no-store',
    ...init,
    headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) },
  });
  const payload = await response.json().catch(() => ({})) as Record<string, unknown>;
  if (!response.ok) {
    throw new HttpFriendRequestError(
      response.status,
      typeof payload.error === 'string' ? payload.error : `请求失败 (${response.status})`,
    );
  }
  return payload as T;
};

type FriendWire = {
  owner_id: unknown;
  display_name: unknown;
  avatar_id: unknown;
  level: unknown;
  relation_id: unknown;
  relation_epoch: unknown;
  action_mask: unknown;
  opportunity_status: unknown;
  opportunity_revision: unknown;
  theft_eligible_targets: unknown;
  validated_at_ms: unknown;
};

const parseUint64 = (value: unknown, field: string, allowZero = false): bigint => {
  if (typeof value !== 'string' || !/^\d+$/.test(value)) throw new Error(`好友资料 ${field} malformed`);
  const parsed = BigInt(value);
  if ((!allowZero && parsed <= 0n) || parsed > 18_446_744_073_709_551_615n) throw new Error(`好友资料 ${field} malformed`);
  return parsed;
};

const parseFriendSummary = (friend: FriendWire): FriendSummary => {
  if (typeof friend.owner_id !== 'string' || !/^\d+$/.test(friend.owner_id)) {
    throw new Error('好友资料 owner_id malformed');
  }
  if (typeof friend.display_name !== 'string' || friend.display_name.trim().length === 0 || friend.display_name.length > 64) {
    throw new Error('好友资料 display_name malformed');
  }
  if (!Number.isInteger(friend.avatar_id) || (friend.avatar_id as number) < 0) {
    throw new Error('好友资料 avatar_id malformed');
  }
  if (!Number.isInteger(friend.level) || (friend.level as number) < 1) {
    throw new Error('好友资料 level malformed');
  }
  if (typeof friend.relation_id !== 'string' || friend.relation_id.trim().length === 0 || friend.relation_id.length > 128) {
    throw new Error('好友资料 relation_id malformed');
  }
  if (!Number.isInteger(friend.action_mask) || (friend.action_mask as number) < 0 || (friend.action_mask as number) > 15) {
    throw new Error('好友资料 action_mask malformed');
  }
  if (friend.opportunity_status !== 'READY' && friend.opportunity_status !== 'UNKNOWN') {
    throw new Error('好友资料 opportunity_status malformed');
  }
  if (!Array.isArray(friend.theft_eligible_targets)) throw new Error('好友资料 theft_eligible_targets malformed');
  const theftEligibleTargets = friend.theft_eligible_targets.map((raw) => {
    if (!raw || typeof raw !== 'object') throw new Error('好友资料 theft target malformed');
    const target = raw as { plot_id?: unknown; crop_cycle_id?: unknown };
    if (!Number.isInteger(target.plot_id) || (target.plot_id as number) < 1 || (target.plot_id as number) > 24
      || typeof target.crop_cycle_id !== 'string') throw new Error('好友资料 theft target malformed');
    try { validateStableOpaqueId(target.crop_cycle_id); } catch { throw new Error('好友资料 theft target malformed'); }
    return { plotId: target.plot_id as number, cropCycleId: target.crop_cycle_id };
  });
  const targetPlots = new Set(theftEligibleTargets.map((target) => target.plotId));
  const targetCycles = new Set(theftEligibleTargets.map((target) => target.cropCycleId));
  if (targetPlots.size !== theftEligibleTargets.length) throw new Error('好友资料 theft target plot duplicate');
  if (targetCycles.size !== theftEligibleTargets.length) throw new Error('好友资料 theft target crop_cycle_id duplicate');
  const hasTheft = ((friend.action_mask as number) & FriendActionMask.THEFT) !== 0;
  if (friend.opportunity_status === 'UNKNOWN' ? theftEligibleTargets.length !== 0 : hasTheft !== (theftEligibleTargets.length > 0)) {
    throw new Error('好友资料 theft target mask mismatch');
  }
  if (!Number.isSafeInteger(friend.validated_at_ms) || (friend.validated_at_ms as number) <= 0) {
    throw new Error('好友资料 validated_at_ms malformed');
  }
  return {
    ownerId: friend.owner_id,
    displayName: friend.display_name.trim(),
    avatarId: friend.avatar_id as number,
    level: friend.level as number,
    relationId: friend.relation_id.trim(),
    relationEpoch: parseUint64(friend.relation_epoch, 'relation_epoch'),
    actionMask: friend.action_mask as number,
    opportunityStatus: friend.opportunity_status,
    opportunityRevision: parseUint64(friend.opportunity_revision, 'opportunity_revision', true),
    theftEligibleTargets,
    validatedAtMs: friend.validated_at_ms as number,
  };
};

/** Server-backed game-friend roster and share acceptance for canonical modes. */
export class HttpGameFriendService implements GameFriendService {
  private readonly relationships = new Set<string>();

  constructor(private readonly apiBase = '/api') {}

  async list(viewerId: string): Promise<FriendSummary[]> {
    return (await this.listPage(viewerId)).friends;
  }

  async listPage(viewerId: string, cursor = '', pageSize?: number): Promise<FriendRosterPage> {
    const query = new URLSearchParams();
    if (cursor) query.set('cursor', cursor);
    if (pageSize !== undefined) query.set('page_size', String(pageSize));
    const suffix = query.size ? `?${query.toString()}` : '';
    const response = await jsonRequest<{
      friends: FriendWire[];
      next_cursor?: unknown;
      roster_revision?: unknown;
      validated_at_ms?: unknown;
    }>(`${this.apiBase}/v1/friends${suffix}`);
    if (!Array.isArray(response.friends)) throw new Error('好友列表 friends malformed');
    if (response.next_cursor !== undefined && typeof response.next_cursor !== 'string') throw new Error('好友列表 next_cursor malformed');
    const rosterRevision = parseUint64(response.roster_revision ?? '0', 'roster_revision', true);
    if (!Number.isSafeInteger(response.validated_at_ms) || (response.validated_at_ms as number) <= 0) {
      throw new Error('好友列表 validated_at_ms malformed');
    }
    const friends = response.friends.map(parseFriendSummary);
    for (const friend of friends) this.relationships.add(relationshipKey(viewerId, friend.ownerId));
    return {
      friends,
      nextCursor: response.next_cursor ?? '',
      rosterRevision,
      validatedAtMs: response.validated_at_ms as number,
    };
  }

  async nextOpportunity(viewerId: string): Promise<FriendOpportunity> {
    const controller = new AbortController();
    const timeout = globalThis.setTimeout(() => controller.abort(), 2_000);
    try {
      // Server owns visited state and advances it only after a real friend
      // SUBSCRIPTION_READY fence. H5 submits neither cursor nor visited IDs.
      const response = await jsonRequest<{ status: unknown; friend?: FriendWire }>(
        `${this.apiBase}/v1/friends/opportunity`, { signal: controller.signal },
      );
      if (response.status === 'FOUND' && response.friend) {
        const friend = parseFriendSummary(response.friend);
        if (friend.opportunityStatus !== 'READY'
          || (friend.actionMask & (FriendActionMask.THEFT | FriendActionMask.WATER | FriendActionMask.PEST)) === 0) {
          throw new Error('好友互动机会 authority result malformed');
        }
        this.relationships.add(relationshipKey(viewerId, friend.ownerId));
        return { status: 'FOUND', friend };
      }
      if (response.status === 'NONE' && response.friend === undefined) return { status: 'NONE' };
      if (response.status === 'UNKNOWN') return { status: 'UNKNOWN' };
      throw new Error('好友互动机会 response malformed');
    } finally {
      globalThis.clearTimeout(timeout);
    }
  }

  async resolve(token: string): Promise<SharePreview> {
    const response = await jsonRequest<{ inviter_label: string }>(
      `${this.apiBase}/v1/share/resolve?token=${encodeURIComponent(token)}`,
    );
    return { inviterLabel: response.inviter_label };
  }

  async accept(token: string, viewerId: string): Promise<ShareResolution> {
    const response = await jsonRequest<{ target_owner_id: string; inviter_label: string }>(
      `${this.apiBase}/v1/share/accept`,
      { method: 'POST', body: JSON.stringify({ share_token: token }) },
    );
    this.relationships.add(relationshipKey(viewerId, response.target_owner_id));
    return { targetOwnerId: response.target_owner_id, inviterLabel: response.inviter_label };
  }

  async revoke(viewerId: string, friendOwnerId: string): Promise<void> {
    const operationId = createUuid();
    await jsonRequest(`${this.apiBase}/v1/friends/revoke`, {
      method: 'POST',
      body: JSON.stringify({ friend_owner_id: friendOwnerId, operation_id: operationId }),
    });
    this.relationships.delete(relationshipKey(viewerId, friendOwnerId));
  }

  hasFriendship(viewerId: string, friendOwnerId: string): boolean {
    return this.relationships.has(relationshipKey(viewerId, friendOwnerId));
  }
}

export class HttpShareLinkCreator implements ShareLinkCreator {
  constructor(private readonly apiBase = '/api') {}

  async create(_viewerId: string, origin: string): Promise<{ url: string }> {
    const response = await jsonRequest<{ share_token: string }>(`${this.apiBase}/v1/share/create`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
    const url = new URL('/', origin);
    url.searchParams.set('share_token', response.share_token);
    return { url: url.toString() };
  }
}

/**
 * The creator remains a separate boundary because creating a share does not
 * establish a friendship. Production must provide its final HTTPS contract.
 */
export type GameShareCreator = ShareLinkCreator;
