import { describe, expect, it } from 'vitest';
import { parseFarmAppProfile } from '../app/environment';

describe('farm app profile', () => {
  it('accepts only local, test, and showcase', () => {
    expect(parseFarmAppProfile('local', 'showcase')).toBe('local');
    expect(parseFarmAppProfile('TEST', 'showcase')).toBe('test');
    expect(parseFarmAppProfile('showcase', 'local')).toBe('showcase');
    expect(() => parseFarmAppProfile('production', 'showcase')).toThrow('unsupported FARM_APP_PROFILE');
  });
});
