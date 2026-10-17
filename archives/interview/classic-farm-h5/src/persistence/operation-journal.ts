/**
 * H5 Operation Journal(28 §3.9,L2-PROTO-22/29/42)。
 *
 * 可恢复写命令在第一次发送前把稳定重放材料写入本 Journal;终态
 * (COMPLETED/EXPIRED/确定性 REJECTED/NO_CHANGE)确认后及时删除。
 * 不得持久化 SessionPermit/TrustedContext/Permit/RouteHeader/旧 subscription_id。
 *
 * 四边界(显式配置、可度量):
 * - maxPendingEntriesPerPlayer:达到上限 → JOURNAL_CAPACITY_BLOCKED,停新写,继续恢复;
 * - maxEntryBytes:单条超限在第一次发送前即拒绝该命令(不先发网络再记 Journal);
 * - maxTotalBytesPerPlayer:同上 Fail Closed;
 * - maxRetentionMs:告警与强制对账期限(JOURNAL_RECOVERY_OVERDUE),不是自动删除 TTL。
 *
 * payloadSchemaVersion 未知(未来 schema)的记录:保留原始 bytes,Fail Closed,
 * 只允许查询,不允许猜测重编码后自动重放。
 */

export const JOURNAL_PAYLOAD_SCHEMA_VERSION = 'p1b-v1';

export type OperationRootPolicy = 'PLAYER' | 'FARM' | 'FRIEND_PAIR' | 'FRIEND_SHARE_ISSUER' | 'PLAYER_MAIL_ENTRY';

export interface PendingOperationRecord {
  environment: string;
  playerId: string;
  operationId: string;
  commandType: string;
  /** 农场命令必填;好友 Pair 命令填 peer_player_id;邮件 Root 命令填 mail_id。 */
  targetFarmId?: string;
  /** Optional local index for rendering/recovery diagnostics; never sent as an authority hint. */
  plotId?: number;
  peerPlayerId?: string;
  mailId?: string;
  operationRootPolicy: OperationRootPolicy;
  /** 服务端已返回时原样保存;客户端不解析。 */
  serverOperationLocator?: string;
  payloadSchemaVersion: string;
  /** 本批唯一编码:'protobuf-base64'(线上确切发送的 Typed Payload bytes)。 */
  payloadEncoding: string;
  originalTypedPayload: string;
  /** 重放重建材料(与 originalTypedPayload 同源的 IR JSON;重编码字节恒等)。 */
  payloadIr?: string;
  /** SCB-1 Canonical Payload Hash(hex);与 FirstHop 黄金规则一致。 */
  canonicalPayloadHash: string;
  createdAtMs: number;
  /** 'Created'(从未上线)/'AwaitingResult'/'Processing'/'Unknown'。 */
  lastStatus: string;
  lastLookupStatus?: 'NOT_FOUND' | 'PROCESSING' | 'COMPLETED' | 'EXPIRED' | 'UNAVAILABLE';
  retryCount: number;
}

export type JournalCapacityState = 'OK' | 'JOURNAL_CAPACITY_BLOCKED' | 'JOURNAL_RECOVERY_OVERDUE';

export interface JournalLimits {
  maxPendingEntriesPerPlayer: number;
  maxEntryBytes: number;
  maxTotalBytesPerPlayer: number;
  maxRetentionMs: number;
}

export const DEFAULT_JOURNAL_LIMITS: JournalLimits = {
  maxPendingEntriesPerPlayer: 64,
  maxEntryBytes: 8 * 1024,
  maxTotalBytesPerPlayer: 256 * 1024,
  maxRetentionMs: 7 * 24 * 60 * 60 * 1000,
};

export class JournalCapacityError extends Error {
  constructor(public readonly state: Exclude<JournalCapacityState, 'OK'>, message: string) {
    super(message);
    this.name = 'JournalCapacityError';
  }
}

export interface JournalMetrics {
  pendingEntries: number;
  totalBytes: number;
  oldestAgeMs: number;
  capacityState: JournalCapacityState;
}

export interface OperationJournal {
  put(record: PendingOperationRecord): Promise<void>;
  remove(scope: string, operationId: string): Promise<void>;
  list(scope: string): Promise<PendingOperationRecord[]>;
}

export const playerScopeOf = (record: Pick<PendingOperationRecord, 'environment' | 'playerId'>): string =>
  `${record.environment}:${record.playerId}`;

const keyOf = (record: PendingOperationRecord): string =>
  `${playerScopeOf(record)}:${record.operationId}`;

const entryBytes = (record: PendingOperationRecord): number =>
  record.originalTypedPayload.length + record.canonicalPayloadHash.length
  + (record.serverOperationLocator?.length ?? 0) + 256;

/**
 * 带四边界 Fail Closed 的 Journal 装饰器。容量/逾期判定基于"写入后"的
 * 投影值;对已有未决记录的更新(状态/Locator)不触发新写拦截。
 */
export class BoundedOperationJournal implements OperationJournal {
  constructor(
    private readonly inner: OperationJournal,
    private readonly limits: JournalLimits = DEFAULT_JOURNAL_LIMITS,
    private readonly now: () => number = Date.now,
  ) {}

  async put(record: PendingOperationRecord): Promise<void> {
    if (entryBytes(record) > this.limits.maxEntryBytes) {
      throw new JournalCapacityError('JOURNAL_CAPACITY_BLOCKED', 'operation journal entry exceeds max_entry_bytes');
    }
    const scope = playerScopeOf(record);
    const existing = await this.inner.list(scope);
    const replacing = existing.find((candidate) => candidate.operationId === record.operationId);
    if (!replacing) {
      const projectedEntries = existing.length + 1;
      const projectedBytes = existing.reduce((sum, candidate) => sum + entryBytes(candidate), 0) + entryBytes(record);
      if (projectedEntries > this.limits.maxPendingEntriesPerPlayer || projectedBytes > this.limits.maxTotalBytesPerPlayer) {
        throw new JournalCapacityError('JOURNAL_CAPACITY_BLOCKED', 'operation journal capacity is exhausted');
      }
    }
    await this.inner.put(record);
  }

  remove(scope: string, operationId: string): Promise<void> {
    return this.inner.remove(scope, operationId);
  }

  list(scope: string): Promise<PendingOperationRecord[]> {
    return this.inner.list(scope);
  }

  async metrics(scope: string): Promise<JournalMetrics> {
    const records = await this.inner.list(scope);
    const now = this.now();
    const totalBytes = records.reduce((sum, record) => sum + entryBytes(record), 0);
    const oldestAgeMs = records.reduce((oldest, record) => Math.max(oldest, now - record.createdAtMs), 0);
    let capacityState: JournalCapacityState = 'OK';
    if (
      records.length >= this.limits.maxPendingEntriesPerPlayer
      || totalBytes >= this.limits.maxTotalBytesPerPlayer
    ) capacityState = 'JOURNAL_CAPACITY_BLOCKED';
    else if (oldestAgeMs > this.limits.maxRetentionMs) capacityState = 'JOURNAL_RECOVERY_OVERDUE';
    return { pendingEntries: records.length, totalBytes, oldestAgeMs, capacityState };
  }
}

export class IndexedDbOperationJournal implements OperationJournal {
  private readonly memory = new Map<string, PendingOperationRecord>();
  private readonly dbPromise: Promise<IDBDatabase | null>;

  constructor(private readonly databaseName = 'classic-farm-h5', private readonly storeName = 'pending-operations') {
    this.dbPromise = this.open();
  }

  async put(record: PendingOperationRecord): Promise<void> {
    this.memory.set(keyOf(record), record);
    const db = await this.dbPromise;
    if (!db) return;
    await this.transaction(db, 'readwrite', (store) => {
      store.put(record, keyOf(record));
    });
  }

  async remove(scope: string, operationId: string): Promise<void> {
    for (const [key, record] of this.memory) {
      if (playerScopeOf(record) === scope && record.operationId === operationId) this.memory.delete(key);
    }
    const db = await this.dbPromise;
    if (!db) return;
    await this.transaction(db, 'readwrite', (store) => {
      const cursorRequest = store.openCursor();
      cursorRequest.onsuccess = () => {
        const cursor = cursorRequest.result;
        if (!cursor) return;
        const record = cursor.value as PendingOperationRecord;
        if (playerScopeOf(record) === scope && record.operationId === operationId) cursor.delete();
        cursor.continue();
      };
    });
  }

  async list(scope: string): Promise<PendingOperationRecord[]> {
    const memoryRecords = [...this.memory.entries()]
      .filter(([key]) => key.startsWith(`${scope}:`))
      .map(([, value]) => value);
    const db = await this.dbPromise;
    if (!db) return memoryRecords;
    return new Promise((resolve, reject) => {
      const request = db.transaction(this.storeName, 'readonly').objectStore(this.storeName).openCursor();
      const records: PendingOperationRecord[] = [];
      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) {
          const merged = new Map<string, PendingOperationRecord>();
          for (const record of memoryRecords) merged.set(record.operationId, record);
          for (const record of records) merged.set(record.operationId, record);
          resolve([...merged.values()]);
          return;
        }
        const record = cursor.value as PendingOperationRecord;
        if (playerScopeOf(record) === scope) records.push(record);
        cursor.continue();
      };
    });
  }

  private open(): Promise<IDBDatabase | null> {
    if (!('indexedDB' in globalThis)) return Promise.resolve(null);
    return new Promise((resolve) => {
      const request = indexedDB.open(this.databaseName, 1);
      request.onupgradeneeded = () => request.result.createObjectStore(this.storeName);
      request.onerror = () => resolve(null);
      request.onsuccess = () => resolve(request.result);
    });
  }

  private transaction(db: IDBDatabase, mode: IDBTransactionMode, operation: (store: IDBObjectStore) => void): Promise<void> {
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(this.storeName, mode);
      operation(transaction.objectStore(this.storeName));
      transaction.oncomplete = () => resolve();
      transaction.onerror = () => reject(transaction.error);
    });
  }
}
