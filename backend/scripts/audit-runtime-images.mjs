import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const manifestPath = join(root, 'deploy/images/r1-1-image-manifest.json');
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
const expectedNames = ['farm-game-runtime', 'farm-load-tools', 'farm-fault-tools'];
const images = new Map(manifest.images.map((image) => [image.name, image]));

if (manifest.schema_version !== 1) throw new Error('unsupported image manifest schema');
if (JSON.stringify([...images.keys()].sort()) !== JSON.stringify(expectedNames.sort())) {
  throw new Error(`image manifest names are not exactly ${expectedNames.join(', ')}`);
}

const binaryExists = (binary) => existsSync(join(root, 'cmd', binary, 'main.go'));
for (const image of manifest.images) {
  const dockerfile = readFileSync(join(root, image.dockerfile), 'utf8');
  for (const binary of image.binaries) {
    if (!binaryExists(binary)) throw new Error(`${image.name} references missing cmd/${binary}/main.go`);
    if (!dockerfile.includes(binary)) throw new Error(`${image.name} Dockerfile does not declare ${binary}`);
  }
}

const runtime = images.get('farm-game-runtime');
const toolBinaries = new Set([
  ...images.get('farm-load-tools').binaries,
  ...images.get('farm-fault-tools').binaries,
]);
for (const binary of runtime.binaries) {
  if (toolBinaries.has(binary)) throw new Error(`runtime image overlaps tool binary ${binary}`);
}
for (const forbidden of runtime.forbidden_binaries) {
  if (runtime.binaries.includes(forbidden)) throw new Error(`runtime image contains forbidden ${forbidden}`);
  if (readFileSync(join(root, runtime.dockerfile), 'utf8').includes(`/cmd/${forbidden}`)) {
    throw new Error(`runtime Dockerfile builds forbidden ${forbidden}`);
  }
}

console.log('runtime image static contents audit passed');
for (const image of manifest.images) {
  console.log(`${image.name}: ${image.binaries.join(', ')}`);
}
