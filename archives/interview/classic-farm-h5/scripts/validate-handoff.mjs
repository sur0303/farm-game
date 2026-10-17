const args = process.argv.slice(2);
const options = {};
for (let index = 0; index < args.length; index += 1) {
  const key = args[index];
  if (!key.startsWith('--')) throw new Error(`unexpected argument: ${key}`);
  options[key.slice(2)] = args[index + 1];
  index += 1;
}

const required = ['environment', 'h5-origin', 'api-origin', 'ws-url'];
const missing = required.filter((key) => !options[key]);
if (missing.length) throw new Error(`missing handoff fields: ${missing.join(', ')}`);
if (!['staging', 'production'].includes(options.environment)) throw new Error('environment must be staging or production');

function parseUrl(value, name, protocols) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${name} must be an absolute URL: ${value}`);
  }
  if (!protocols.includes(url.protocol)) throw new Error(`${name} must use ${protocols.join(' or ')}: ${value}`);
  if (url.username || url.password) throw new Error(`${name} must not contain credentials`);
  if (/(^|\.)example\./i.test(url.hostname) || /\.(?:test|invalid|localhost)$/i.test(url.hostname)) {
    throw new Error(`${name} still uses a placeholder host`);
  }
  return url;
}

const h5 = parseUrl(options['h5-origin'], 'h5-origin', ['https:']);
const api = parseUrl(options['api-origin'], 'api-origin', ['https:']);
const ws = parseUrl(options['ws-url'], 'ws-url', ['wss:']);
if (h5.hash || api.hash || ws.hash) throw new Error('handoff URLs must not contain fragments');
if (h5.pathname !== '/' || h5.search) throw new Error('h5-origin must be an origin without a path or query');
if (api.pathname !== '/' || api.search) throw new Error('api-origin must be an origin without a path or query');

console.log('handoff validation passed');
console.log(`environment=${options.environment}`);
console.log(`H5_ORIGIN=${h5.origin}${h5.pathname === '/' ? '' : h5.pathname}`);
console.log(`API_ORIGIN=${api.origin}${api.pathname === '/' ? '' : api.pathname}`);
console.log(`WS_URL=${ws.toString()}`);
console.log('next: copy API_ORIGIN and WS_URL into the staging/production deployment configuration, then run npm run audit:build');