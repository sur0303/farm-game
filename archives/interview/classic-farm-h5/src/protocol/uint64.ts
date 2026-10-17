/** JavaScript boundary helpers: canonical uint64 values remain bigint/string. */
export type Uint64 = bigint;
export type Uint64Input = string | number | bigint;

export const parseUint64 = (value: Uint64Input, field = 'uint64'): bigint => {
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${field} is not an exact non-negative integer`);
    return BigInt(value);
  }
  const parsed = typeof value === 'bigint' ? value : BigInt(value);
  if (parsed < 0n || parsed > 0xffffffffffffffffn) throw new Error(`${field} is outside uint64 range`);
  return parsed;
};

export const uint64String = (value: bigint, field = 'uint64'): string => {
  if (value < 0n || value > 0xffffffffffffffffn) throw new Error(`${field} is outside uint64 range`);
  return value.toString(10);
};

export const compareUint64 = (left: Uint64Input, right: Uint64Input): -1 | 0 | 1 => {
  const a = parseUint64(left, 'left');
  const b = parseUint64(right, 'right');
  return a < b ? -1 : a > b ? 1 : 0;
};

export const isNextUint64 = (current: Uint64Input, next: Uint64Input): boolean => {
  return parseUint64(next, 'next') === parseUint64(current, 'current') + 1n;
};
