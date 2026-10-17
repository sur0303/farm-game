export interface BinaryMessageEvent {
  data: ArrayBuffer;
}

export interface BinaryCloseEvent {
  code: number;
  reason: string;
  wasClean: boolean;
}

export interface BinaryTransport {
  binaryType: 'arraybuffer';
  readyState: 0 | 1 | 2 | 3;
  onopen: (() => void) | null;
  onmessage: ((event: BinaryMessageEvent) => void) | null;
  onclose: ((event: BinaryCloseEvent) => void) | null;
  onerror: ((error: unknown) => void) | null;
  open?(): void;
  send(data: ArrayBuffer): void;
  close(code?: number, reason?: string): void;
}

export class BrowserWebSocketTransport implements BinaryTransport {
  readonly binaryType = 'arraybuffer' as const;
  onopen: (() => void) | null = null;
  onmessage: ((event: BinaryMessageEvent) => void) | null = null;
  onclose: ((event: BinaryCloseEvent) => void) | null = null;
  onerror: ((error: unknown) => void) | null = null;
  private readonly socket: WebSocket;

  constructor(url: string) {
    this.socket = new WebSocket(url);
    this.socket.binaryType = 'arraybuffer';
    this.socket.onopen = () => this.onopen?.();
    this.socket.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        this.onmessage?.({ data: event.data });
      } else if (typeof event.data === 'string') {
        this.onerror?.(new Error(`FarmGate sent an unexpected text frame: ${event.data.slice(0, 128)}`));
      } else {
        this.onerror?.(new Error('server sent non-binary WebSocket data'));
      }
    };
    this.socket.onclose = (event) => this.onclose?.({
      code: event.code,
      reason: event.reason,
      wasClean: event.wasClean,
    });
    this.socket.onerror = (event) => this.onerror?.(event);
  }

  get readyState(): 0 | 1 | 2 | 3 {
    return this.socket.readyState as 0 | 1 | 2 | 3;
  }

  send(data: ArrayBuffer): void {
    this.socket.send(data);
  }

  close(code?: number, reason?: string): void {
    this.socket.close(code, reason);
  }
}
