export interface ConnectionTicket {
  value: string;
  playerId: string;
  sessionEpoch: string;
  environment: string;
  wsUrl?: string;
}

export interface AuthProvider {
  acquireConnectionTicket(signal?: AbortSignal): Promise<ConnectionTicket>;
}

export function parseConnectionTicket(value: unknown): ConnectionTicket {
  if (!value || typeof value !== 'object') throw new Error('connection ticket response is invalid');
  const candidate = value as Record<string, unknown>;
  const readString = (...keys: string[]): string | undefined => {
    for (const key of keys) {
      if (typeof candidate[key] === 'string' && candidate[key]) return candidate[key] as string;
    }
    return undefined;
  };
  const ticketValue = readString('value', 'connection_ticket');
  const playerId = readString('playerId', 'player_id');
  const sessionEpoch = readString('sessionEpoch', 'reserved_session_epoch', 'active_session_epoch');
  const environment = readString('environment', 'platform') ?? 'web';
  const wsUrl = readString('wsUrl', 'wss_endpoint');
  if (!ticketValue) throw new Error('connection ticket value is missing');
  if (!playerId) throw new Error('connection ticket playerId is missing');
  if (!sessionEpoch) throw new Error('connection ticket sessionEpoch is missing');
  return {
    value: ticketValue,
    playerId,
    sessionEpoch,
    environment,
    ...(wsUrl ? { wsUrl } : {}),
  };
}

