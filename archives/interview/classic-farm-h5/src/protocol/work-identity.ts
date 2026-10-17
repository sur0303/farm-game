// This module is a cross-language compatibility/test implementation. The
// server remains authoritative for every workflow and storage identity.
const textEncoder = new TextEncoder();

const STABLE_ID_PREFIX = 'sid1_';
const WORK_REF_PREFIX = 'wr1_';
const RECEIPT_REF_PREFIX = 'rr1_';
const SHA256_BYTES = 32;
const MAX_UINT64 = (1n << 64n) - 1n;

export class WorkIdentityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorkIdentityError';
  }
}

export interface WorkRefV1 {
  workType: string;
  workId: string;
}

export interface ReceiptRefV1 {
  actorId: string;
  receiptKind: string;
  operationDigest: string;
}

export async function stableOpaqueId(namespace: string, ...fields: string[]): Promise<string> {
  if (namespace.length === 0) {
    throw new WorkIdentityError('namespace is required');
  }
  const digest = await sha256(encodeLengthPrefixed([namespace, ...fields]));
  return STABLE_ID_PREFIX + encodeBase64Url(digest);
}

export async function workflowId(
  subjectPlayerId: string,
  commandType: string,
  operationId: string,
): Promise<string> {
  requireCanonicalUint64(subjectPlayerId, 'subject_player_id');
  requireStableName(commandType, 'command_type');
  if (operationId.length === 0) {
    throw new WorkIdentityError('operation_id is required');
  }
  return stableOpaqueId('farm-workflow-id-v1', subjectPlayerId, commandType, operationId);
}

export async function operationDigest(
  subjectPlayerId: string,
  commandType: string,
  operationId: string,
): Promise<string> {
  requireCanonicalUint64(subjectPlayerId, 'subject_player_id');
  requireStableName(commandType, 'command_type');
  if (operationId.length === 0) {
    throw new WorkIdentityError('operation_id is required');
  }
  return bytesToHex(await sha256(encodeLengthPrefixed([subjectPlayerId, commandType, operationId])));
}

export function validateStableOpaqueId(id: string): void {
  if (!id.startsWith(STABLE_ID_PREFIX)) {
    throw new WorkIdentityError('invalid stable id prefix');
  }
  const payload = id.slice(STABLE_ID_PREFIX.length);
  let decoded: Uint8Array;
  try {
    decoded = decodeBase64Url(payload);
  } catch {
    throw new WorkIdentityError('invalid stable id encoding');
  }
  if (decoded.length !== SHA256_BYTES || STABLE_ID_PREFIX + encodeBase64Url(decoded) !== id) {
    throw new WorkIdentityError('invalid stable id encoding');
  }
}

export function encodeWorkRefV1(workType: string, workId: string): string {
  requireStableName(workType, 'work_type');
  validateStableOpaqueId(workId);
  return WORK_REF_PREFIX + encodeBase64Url(encodeLengthPrefixed([workType, workId]));
}

export function decodeWorkRefV1(encoded: string): WorkRefV1 {
  const [workType, workId] = decodeReference(encoded, WORK_REF_PREFIX, 2);
  const result = { workType, workId };
  if (encodeWorkRefV1(result.workType, result.workId) !== encoded) {
    throw new WorkIdentityError('non-canonical work ref');
  }
  return result;
}

export function encodeReceiptRefV1(
  actorId: string,
  receiptKind: string,
  digest: string,
): string {
  requireCanonicalUint64(actorId, 'actor_id');
  requireStableName(receiptKind, 'receipt_kind');
  if (!/^[0-9a-f]{64}$/.test(digest)) {
    throw new WorkIdentityError('operation_digest must be lowercase SHA-256 hex');
  }
  return RECEIPT_REF_PREFIX + encodeBase64Url(
    encodeLengthPrefixed([actorId, receiptKind, digest]),
  );
}

export function decodeReceiptRefV1(encoded: string): ReceiptRefV1 {
  const [actorId, receiptKind, digest] = decodeReference(encoded, RECEIPT_REF_PREFIX, 3);
  const result = { actorId, receiptKind, operationDigest: digest };
  if (encodeReceiptRefV1(result.actorId, result.receiptKind, result.operationDigest) !== encoded) {
    throw new WorkIdentityError('non-canonical receipt ref');
  }
  return result;
}

function encodeLengthPrefixed(values: string[]): Uint8Array {
  for (const value of values) {
    requireWellFormedUnicode(value);
  }
  const encoded = values.map((value) => textEncoder.encode(value));
  let total = 0;
  for (const value of encoded) {
    if (value.length > 0xffff_ffff) {
      throw new WorkIdentityError('length-prefixed field is too large');
    }
    total += 4 + value.length;
  }
  const output = new Uint8Array(total);
  const view = new DataView(output.buffer);
  let offset = 0;
  for (const value of encoded) {
    view.setUint32(offset, value.length, false);
    offset += 4;
    output.set(value, offset);
    offset += value.length;
  }
  return output;
}

function decodeReference(encoded: string, prefix: string, expectedFields: number): string[] {
  if (!encoded.startsWith(prefix) || encoded.length > 1024) {
    throw new WorkIdentityError('invalid reference prefix or size');
  }
  const payloadText = encoded.slice(prefix.length);
  const payload = decodeBase64Url(payloadText);
  if (prefix + encodeBase64Url(payload) !== encoded) {
    throw new WorkIdentityError('non-canonical base64url reference');
  }
  const fields: string[] = [];
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  let offset = 0;
  while (offset < payload.length) {
    if (payload.length - offset < 4 || fields.length >= expectedFields) {
      throw new WorkIdentityError('invalid reference field count');
    }
    const length = view.getUint32(offset, false);
    offset += 4;
    if (length > payload.length - offset) {
      throw new WorkIdentityError('invalid reference field length');
    }
    fields.push(new TextDecoder('utf-8', { fatal: true }).decode(payload.subarray(offset, offset + length)));
    offset += length;
  }
  if (fields.length !== expectedFields) {
    throw new WorkIdentityError('invalid reference field count');
  }
  return fields;
}

async function sha256(input: Uint8Array): Promise<Uint8Array> {
  if (!globalThis.crypto?.subtle) {
    throw new WorkIdentityError('Web Crypto SHA-256 is unavailable');
  }
  const buffer = new ArrayBuffer(input.byteLength);
  new Uint8Array(buffer).set(input);
  return new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', buffer));
}

function requireStableName(value: string, field: string): void {
  if (!/^[A-Z0-9_]{1,64}$/.test(value)) {
    throw new WorkIdentityError(`${field} is not a stable name`);
  }
}

function requireCanonicalUint64(value: string, field: string): void {
  if (!/^[1-9][0-9]{0,19}$/.test(value)) {
    throw new WorkIdentityError(`${field} is not canonical uint64`);
  }
  const parsed = BigInt(value);
  if (parsed > MAX_UINT64) {
    throw new WorkIdentityError(`${field} exceeds uint64`);
  }
}

function requireWellFormedUnicode(value: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const current = value.charCodeAt(index);
    if (current >= 0xd800 && current <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        throw new WorkIdentityError('identity field contains an unpaired surrogate');
      }
      index += 1;
    } else if (current >= 0xdc00 && current <= 0xdfff) {
      throw new WorkIdentityError('identity field contains an unpaired surrogate');
    }
  }
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
}

function encodeBase64Url(bytes: Uint8Array): string {
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/u, '');
}

function decodeBase64Url(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]*$/.test(value) || value.length % 4 === 1) {
    throw new WorkIdentityError('invalid base64url');
  }
  const standard = value.replace(/-/g, '+').replace(/_/g, '/');
  const padded = standard + '='.repeat((4 - (standard.length % 4)) % 4);
  let binary: string;
  try {
    binary = atob(padded);
  } catch {
    throw new WorkIdentityError('invalid base64url');
  }
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}
