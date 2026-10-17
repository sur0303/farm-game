/**
 * Creates an RFC 4122-shaped v4 UUID in both secure and non-secure browser
 * contexts. `crypto.randomUUID()` is unavailable on plain HTTP origins other
 * than localhost, while `crypto.getRandomValues()` remains available in the
 * browsers supported by the H5 client.
 */
export const createUuid = (): string => {
  const cryptoApi = typeof globalThis.crypto === 'undefined' ? undefined : globalThis.crypto;
  if (typeof cryptoApi?.randomUUID === 'function') return cryptoApi.randomUUID();

  const bytes = new Uint8Array(16);
  if (typeof cryptoApi?.getRandomValues === 'function') {
    cryptoApi.getRandomValues(bytes);
  } else {
    // Last-resort compatibility for old embedded WebViews. These IDs provide
    // idempotency, not authentication; server-issued tickets remain the trust
    // boundary.
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
