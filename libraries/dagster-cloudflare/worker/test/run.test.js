import {test} from 'node:test';
import assert from 'node:assert/strict';
import {RunObject, route, validateLaunch} from '../src/run.js';
import {COMMAND, fakeCtx, request} from './fakes.js';

const flush = () => new Promise(resolve => setImmediate(resolve));

async function launched(env = {}, payload = {command: COMMAND}) {
  const ctx = fakeCtx();
  const run = new RunObject(ctx, env);
  const response = await run.fetch(request('POST', payload));
  return {ctx, run, response, body: await response.json()};
}

test('launch starts one container with the run command, env and instance', async () => {
  const {ctx, response, body} = await launched(
    {RUN_ENV_JSON: '{"A":"base","B":"base"}', DEFAULT_INSTANCE: 'standard-1', ENTRYPOINT_PREFIX: '["/bin/wrap"]'},
    {command: COMMAND, env: {B: 'run'}, instance: {vcpu: 2, memoryMib: 8192, diskMb: 8000}, image: 'big'},
  );
  assert.equal(response.status, 202);
  assert.equal(body.state, 'running');
  assert.deepEqual(ctx.container.started, [{
    image: 'registry/big:1',
    enableInternet: true,
    entrypoint: ['/bin/wrap', ...COMMAND],
    env: {A: 'base', B: 'run'},
    instance: {vcpu: 2, memoryMib: 8192, diskMb: 8000},
  }]);
  assert.ok(ctx.storage.alarmAt > Date.now());
  assert.ok(ctx.container.timeoutMs > 0);
});

test('a repeated launch returns the state and starts nothing new', async () => {
  const {ctx, run} = await launched();
  const again = await run.fetch(request('POST', {command: COMMAND}));
  assert.equal(again.status, 200);
  assert.equal((await again.json()).state, 'running');
  assert.equal(ctx.container.started.length, 1);
});

test('the same run ID with another command is a conflict', async () => {
  const {run} = await launched();
  const other = await run.fetch(request('POST', {command: ['dagster', 'api', 'execute_run', '{}']}));
  assert.equal(other.status, 409);
});

test('exit code 0 is success; non-zero keeps the exit code', async () => {
  const ok = await launched();
  ok.ctx.container.finish(0);
  await flush();
  assert.equal((await ok.run.status()).state, 'success');

  const oom = await launched();
  oom.ctx.container.finish(137);
  await flush();
  assert.deepEqual(await oom.run.status(), {state: 'failed', message: 'Container exited with code 137.', exitCode: 137});
});

test('a container that is gone without an exit report fails at the next heartbeat', async () => {
  const {ctx, run} = await launched();
  ctx.container.running = false; // object restarted, monitor() lost
  run.monitoring = null;
  await run.alarm();
  assert.equal((await run.status()).state, 'failed');
});

test('a heartbeat on a running container re-arms the alarm', async () => {
  const {ctx, run} = await launched();
  ctx.storage.alarmAt = null;
  await run.alarm();
  assert.ok(ctx.storage.alarmAt > Date.now());
  assert.equal((await run.status()).state, 'running');
});

test('cancel sends SIGTERM and ends as canceled', async () => {
  const {ctx, run} = await launched();
  const response = await run.fetch(request('DELETE'));
  const body = await response.json();
  assert.equal(body.terminated, true);
  assert.equal(body.state, 'canceled');
  assert.deepEqual(ctx.container.signals, [15]);
  assert.deepEqual(ctx.container.destroyed, []);
});

test('cancel destroys a container that ignores SIGTERM', async () => {
  const {ctx, run} = await launched({STOP_TIMEOUT_MS: '10'});
  ctx.container.exitOnSigterm = false;
  const body = await (await run.fetch(request('DELETE'))).json();
  assert.equal(body.state, 'canceled');
  assert.equal(ctx.container.destroyed.length, 1);
});

test('cancel of an unknown or finished run does nothing', async () => {
  const ctx = fakeCtx();
  const run = new RunObject(ctx, {});
  assert.equal((await (await run.fetch(request('DELETE'))).json()).terminated, false);
  const done = await launched();
  done.ctx.container.finish(0);
  await flush();
  assert.equal((await (await done.run.fetch(request('DELETE'))).json()).terminated, false);
});

test('a start error is reported as failed', async () => {
  const ctx = fakeCtx();
  ctx.container.startError = new Error('no capacity');
  const run = new RunObject(ctx, {});
  const body = await (await run.fetch(request('POST', {command: COMMAND}))).json();
  assert.equal(body.state, 'failed');
  assert.match(body.message, /no capacity/);
});

test('finished runs are deleted after the retention period', async () => {
  const {ctx, run} = await launched({RETENTION_DAYS: '0'});
  ctx.container.finish(0);
  await flush();
  await run.alarm();
  assert.equal((await run.status()).state, 'missing');
});

test('bad launch requests are rejected', async () => {
  assert.match(validateLaunch({command: ['sh', '-c', 'id']}), /execute_run/);
  assert.match(validateLaunch({command: COMMAND, env: {A: 1}}), /env/);
  assert.match(validateLaunch({command: COMMAND, instance: [1]}), /instance/);
  assert.equal(validateLaunch({command: COMMAND, instance: 'standard-2'}), null);
  const {response} = await launched({}, {command: COMMAND, image: 'nope'});
  assert.equal(response.status, 400);
});

test('router checks the token and the run ID shape', async () => {
  const seen = [];
  const env = {
    DISPATCH_TOKEN: 'secret',
    RUNS: {idFromName: name => name, get: id => ({fetch: () => { seen.push(id); return new Response('ok'); }})},
  };
  assert.equal((await route(request('GET'), env)).status, 200);
  assert.deepEqual(seen, ['0b6f0f8e-1111-4222-8333-944445555666']);
  const bad = new Request('https://w.example/runs/x', {headers: {Authorization: 'Bearer secret'}});
  assert.equal((await route(bad, env)).status, 404);
  const anon = new Request('https://w.example/runs/0b6f0f8e-1111-4222-8333-944445555666');
  assert.equal((await route(anon, env)).status, 401);
  assert.equal((await route(request('GET'), {...env, DISPATCH_TOKEN: ''})).status, 503);
});

const CODE_IMAGES = JSON.stringify({default: 'code@sha256:aaa', big: 'code@sha256:bbb'});

test('without CODE_IMAGES the code image is not checked', async () => {
  const {response, ctx} = await launched({}, {command: COMMAND, code_image: 'anything'});
  assert.equal(response.status, 202);
  assert.equal(ctx.container.started[0].image, 'registry/default:1');
});

test('with CODE_IMAGES a run gets the image built from its code image', async () => {
  const {response, ctx} = await launched({CODE_IMAGES}, {command: COMMAND, code_image: 'code@sha256:bbb'});
  assert.equal(response.status, 202);
  assert.equal(ctx.container.started[0].image, 'registry/big:1');
});

test('a run from another code image is refused and starts nothing', async () => {
  const {response, body, ctx} = await launched({CODE_IMAGES}, {command: COMMAND, code_image: 'code@sha256:new'});
  assert.equal(response.status, 409);
  assert.match(body.error, /No image in this Worker was built from code@sha256:new/);
  assert.equal(ctx.container.started.length, 0);
});

test('CODE_IMAGES as a wrangler object var works like the JSON string', async () => {
  const {response} = await launched({CODE_IMAGES: JSON.parse(CODE_IMAGES)}, {command: COMMAND, code_image: 'code@sha256:new'});
  assert.equal(response.status, 409);
});

test('a named image must match the code image too', async () => {
  const {response, ctx} = await launched({CODE_IMAGES}, {command: COMMAND, image: 'big', code_image: 'code@sha256:aaa'});
  assert.equal(response.status, 409);
  assert.equal(ctx.container.started.length, 0);
});

test('a run without a code image is refused when its image has an entry', async () => {
  const {response, body} = await launched({CODE_IMAGES}, {command: COMMAND});
  assert.equal(response.status, 409);
  assert.match(body.error, /DAGSTER_CURRENT_IMAGE/);
  assert.match(validateLaunch({command: COMMAND, code_image: 1}), /code_image/);
});
