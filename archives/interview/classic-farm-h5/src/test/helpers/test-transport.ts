import type { BinaryCloseEvent, BinaryMessageEvent, BinaryTransport } from '../../network/transport';

class TestEndpoint implements BinaryTransport {
  readonly binaryType = 'arraybuffer' as const;
  readyState: 0 | 1 | 2 | 3 = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: BinaryMessageEvent) => void) | null = null;
  onclose: ((event: BinaryCloseEvent) => void) | null = null;
  onerror: ((error: unknown) => void) | null = null;
  peer: TestEndpoint | null = null;

  open(): void {
    this.readyState = 1;
    queueMicrotask(() => this.onopen?.());
  }

  send(data: ArrayBuffer): void {
    if (this.readyState !== 1 || !this.peer || this.peer.readyState !== 1) {
      this.onerror?.(new Error('test transport is not open'));
      return;
    }
    const copy = data.slice(0);
    queueMicrotask(() => this.peer?.onmessage?.({ data: copy }));
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState === 3) return;
    this.readyState = 2;
    const peer = this.peer;
    queueMicrotask(() => {
      this.readyState = 3;
      this.onclose?.({ code, reason, wasClean: true });
      if (peer && peer.readyState !== 3) peer.closeFromPeer(code, reason);
    });
  }

  private closeFromPeer(code: number, reason: string): void {
    this.readyState = 3;
    this.onclose?.({ code, reason, wasClean: true });
  }
}

export function createTestTransportPair(): [BinaryTransport, BinaryTransport] {
  const client = new TestEndpoint();
  const server = new TestEndpoint();
  client.peer = server;
  server.peer = client;
  return [client, server];
}
