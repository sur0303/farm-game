import type { ClientFrame, ServerFrame } from './types';
export { finalProtocolAdapter, FinalProtocolAdapter } from './final-protocol-adapter';

/**
 * Wire-protocol seam for WsClient.
 *
 * Product builds inject the canonical adapter generated from the
 * server-owned Proto. WsClient has no implicit fallback wire protocol.
 */
export interface FarmProtocolAdapter {
  /**
   * The production adapter requires ACCEPTED + Snapshot + catch-up + Ready.
   * Snapshot-only semantics are available only to isolated test adapters.
   */
  joinBarrier?: 'snapshot-only' | 'accepted-snapshot-ready';
  encodeClientFrame(frame: ClientFrame): Uint8Array;
  decodeServerFrame(bytes: Uint8Array): ServerFrame;
  /** Reset per-connection sequence state before a new transport generation. */
  resetConnection?(): void;
}
