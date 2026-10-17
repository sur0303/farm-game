import { execFileSync } from 'node:child_process';
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const removedRuntimeMarkers = [
  'mock-auth-provider',
  'fake-farm-server',
  'mock-share-player',
  'mock-friend-roster',
  'mock-legacy',
];
const showcaseForbiddenMarkers = [
  'staging-auth-provider',
  'test-auth-provider',
  'demo-platform-token',
  'demo-platform-token-b',
  'mock-auth-provider',
  'mock-friend-provider',
];

function filesUnder(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? filesUnder(path) : [path];
  });
}

function auditMode(buildScript, expectedProfile) {
  execFileSync(npmCommand, ['run', buildScript], {
    cwd: root,
    stdio: 'inherit',
    shell: process.platform === 'win32',
  });
  const files = filesUnder(join(root, 'dist'));
  const content = files
    .filter((file) => /\.(?:js|html|css)$/.test(file))
    .map((file) => readFileSync(file, 'utf8'))
    .join('\n');
  const paths = files.join('\n').toLowerCase();
  const forbiddenMarkers = expectedProfile === 'showcase'
    ? [...removedRuntimeMarkers, ...showcaseForbiddenMarkers]
    : removedRuntimeMarkers;
  const forbidden = forbiddenMarkers.filter((marker) => paths.includes(marker) || content.includes(marker));
  if (forbidden.length) throw new Error(`${buildScript} contains removed local-runtime modules: ${forbidden.join(', ')}`);
  if (expectedProfile === 'test' && !paths.includes('staging-auth-provider')) {
    throw new Error(`${buildScript} dropped the explicit test-only staging auth provider`);
  }
  console.log(`${buildScript} (${expectedProfile}): real-backend boundary audit passed`);
  return { buildScript, profile: expectedProfile, files: files.length };
}

const audits = [
  auditMode('build:web', 'showcase'),
  auditMode('build:staging', 'test'),
  auditMode('build:wechat', 'local'),
];

writeFileSync(join(root, 'dist/r1-1-showcase-build-manifest.json'), `${JSON.stringify({
  schema_version: 1,
  profile: 'showcase',
  build_script: 'build:web',
  auth_flow: 'real web account register/login -> HttpOnly Cookie -> connection ticket -> WSS -> Redis Authority',
  forbidden_runtime_markers: showcaseForbiddenMarkers,
  audits,
}, null, 2)}\n`);

