import { execFileSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const h5 = resolve(here, '..');
const farmGame = process.env.FARM_GAME_DIR
  ? resolve(process.env.FARM_GAME_DIR)
  : resolve(h5, '..', 'farm-game');
const output = resolve(h5, 'src/protocol/generated');
mkdirSync(output, { recursive: true });
const protos = [
  'api/proto/common/control.proto', 'api/proto/common/enums.proto',
  'api/proto/common/envelope.proto', 'api/proto/common/frame.proto',
  'api/proto/economy/economy.proto', 'api/proto/farm/farm.proto',
  'api/proto/farm/farm_view.proto', 'api/proto/friend/friend.proto',
  'api/proto/logicrpc/logic_service.proto',
  'api/proto/player/player.proto',
  'api/proto/public/commands.proto',
  'api/proto/task/task.proto',
  'api/proto/ownercore/ownercore.proto', 'api/proto/settlement/settlement.proto',
  'api/proto/tcapluspb/options.proto',
  'api/proto/workflow/workflow.proto',
].map((p) => resolve(farmGame, p));
execFileSync('protoc', [
  '-I', resolve(farmGame, 'api/proto'), '-I', resolve(farmGame, 'third_party'),
  '--plugin=' + resolve(h5, 'node_modules/.bin/protoc-gen-ts_proto'),
  '--ts_proto_out=' + output,
  '--ts_proto_opt=paths=source_relative,esModuleInterop=true,forceLong=bigint,useOptionals=messages,outputJsonMethods=false',
  ...protos,
], { cwd: h5, stdio: 'inherit' });
