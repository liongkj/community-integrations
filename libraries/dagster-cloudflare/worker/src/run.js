import {ACTIVE, TERMINAL, transition} from './lifecycle.js';

const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;

function config(env) {
  return {
    heartbeatMs: Number(env.HEARTBEAT_MS || 5 * MINUTE),
    stopTimeoutMs: Number(env.STOP_TIMEOUT_MS || 30_000),
    retentionMs: Number(env.RETENTION_DAYS || 7) * DAY,
    defaultImage: env.DEFAULT_IMAGE || 'default',
    defaultInstance: env.DEFAULT_INSTANCE || undefined,
    entrypointPrefix: env.ENTRYPOINT_PREFIX ? JSON.parse(env.ENTRYPOINT_PREFIX) : [],
    baseEnv: env.RUN_ENV_JSON ? JSON.parse(env.RUN_ENV_JSON) : {},
    // Image name -> the code image it was built FROM, e.g. {"default": "repo/app@sha256:..."}.
    // wrangler passes an object var as an object; a secret or .dev.vars value is a string.
    codeImages: typeof env.CODE_IMAGES === 'string' ? JSON.parse(env.CODE_IMAGES) : env.CODE_IMAGES ?? null,
  };
}

export function validateLaunch(payload) {
  const {command, env = {}, image, instance, code_image: codeImage} = payload ?? {};
  if (!Array.isArray(command) || !command.every(x => typeof x === 'string'))
    return 'command must be an array of strings';
  // ExecuteRunArgs.get_command_args() ends with: api execute_run <serialized args>.
  if (command.length < 3 || command.at(-3) !== 'api' || command.at(-2) !== 'execute_run')
    return 'command must be a Dagster "api execute_run" command';
  if (typeof env !== 'object' || Array.isArray(env) || !Object.values(env).every(v => typeof v === 'string'))
    return 'env must be an object of strings';
  if (image !== undefined && typeof image !== 'string') return 'image must be a string';
  if (codeImage !== undefined && typeof codeImage !== 'string') return 'code_image must be a string';
  if (instance !== undefined && typeof instance !== 'string' && (typeof instance !== 'object' || Array.isArray(instance)))
    return 'instance must be a type name or {vcpu, memoryMib, diskMb}';
  return null;
}

// Like the ECS run launcher, the run image follows the code location's image.
// With CODE_IMAGES, a run gets the image built from its code image; a run whose
// code image has no image in this Worker, or a named image built from another
// code image, is refused, so a run never executes code other than its code
// location's. Without CODE_IMAGES: the requested image or DEFAULT_IMAGE, no check.
export function pickImage(cfg, {image, code_image: codeImage}) {
  const codeImages = cfg.codeImages;
  if (!codeImages) return {name: image ?? cfg.defaultImage};
  if (image === undefined) {
    if (codeImage === undefined)
      return {error: 'The run has no code location image. Set DAGSTER_CURRENT_IMAGE on the code server.'};
    const name = Object.keys(codeImages).find(key => codeImages[key] === codeImage);
    if (name === undefined)
      return {error: `No image in this Worker was built from ${codeImage}. Deploy the Worker with an image built from it.`};
    return {name};
  }
  const expected = codeImages[image];
  if (expected === undefined || expected === codeImage) return {name: image};
  return {error: `Image ${JSON.stringify(image)} was built from ${expected}, but the run's code location runs ${codeImage ?? 'no image'}.`};
}

async function sha256(value) {
  const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value)));
  return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
}

// One object per Dagster run. `ctx` and `env` are the Durable Object's.
export class RunObject {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.cfg = config(env);
    this.monitoring = null;
    // monitor() does not survive a Durable Object restart.
    if (ctx.container?.running) this.monitor();
  }

  async advance(event, details = {}) {
    return this.ctx.storage.transaction(async txn => {
      const state = await txn.get('state');
      const result = transition(state, event);
      if (result.state !== state) {
        await txn.put('state', result.state);
        if (details.message) await txn.put('message', details.message);
        if (details.exitCode !== undefined) await txn.put('exitCode', details.exitCode);
        if (TERMINAL.has(result.state)) await txn.put('finishedAt', Date.now());
      }
      return result;
    });
  }

  monitor() {
    if (!this.monitoring) {
      this.monitoring = this.ctx.container.monitor().then(
        () => this.advance('success', {message: 'Container exited with code 0.'}),
        error => {
          const exitCode = typeof error?.exitCode === 'number' ? error.exitCode : undefined;
          const message = exitCode === undefined
            ? `Container stopped: ${error?.message ?? error}`
            : `Container exited with code ${exitCode}.`;
          return this.advance('failed', {message, exitCode});
        },
      ).finally(() => { this.monitoring = null; });
      this.ctx.waitUntil(this.monitoring);
    }
    return this.monitoring;
  }

  async keepAlive() {
    // Keep the container after the object goes idle, at least until the next heartbeat.
    await this.ctx.container.setInactivityTimeout(3 * this.cfg.heartbeatMs);
    await this.ctx.storage.setAlarm(Date.now() + this.cfg.heartbeatMs);
  }

  async alarm() {
    const state = await this.ctx.storage.get('state');
    if (ACTIVE.has(state)) {
      if (this.ctx.container.running) {
        this.monitor();
        await this.keepAlive();
      } else {
        await this.advance('failed', {message: 'Container stopped without an exit report.'});
        await this.ctx.storage.setAlarm(Date.now() + this.cfg.retentionMs);
      }
      return;
    }
    const finishedAt = await this.ctx.storage.get('finishedAt');
    if (finishedAt !== undefined && Date.now() - finishedAt >= this.cfg.retentionMs) {
      await this.ctx.storage.deleteAll();
    } else if (finishedAt !== undefined) {
      await this.ctx.storage.setAlarm(finishedAt + this.cfg.retentionMs);
    }
  }

  async status() {
    const [state, message, exitCode] = await Promise.all(
      ['state', 'message', 'exitCode'].map(key => this.ctx.storage.get(key)),
    );
    return {state: state ?? 'missing', message: message ?? '', exitCode: exitCode ?? null};
  }

  async launch(payload) {
    const invalid = validateLaunch(payload);
    if (invalid) return {status: 400, body: {error: invalid}};
    const picked = pickImage(this.cfg, payload);
    if (picked.error) return {status: 409, body: {error: picked.error}};
    const imageName = picked.name;
    const image = this.ctx.container.images?.[imageName];
    if (!image) return {status: 400, body: {error: `Unknown image ${JSON.stringify(imageName)}`}};

    const digest = await sha256(JSON.stringify(payload.command));
    const start = await this.ctx.storage.transaction(async txn => {
      const state = await txn.get('state');
      if (state && (await txn.get('commandDigest')) !== digest) return {conflict: true};
      const result = transition(state, 'launch');
      if (result.action === 'start') {
        await txn.put('state', result.state);
        await txn.put('commandDigest', digest);
      }
      return result;
    });
    if (start.conflict) return {status: 409, body: {error: 'Run ID already used with a different command'}};
    if (start.action !== 'start') return {status: 200, body: await this.status()};

    const options = {
      image,
      enableInternet: true,
      entrypoint: [...this.cfg.entrypointPrefix, ...payload.command],
      env: {...this.cfg.baseEnv, ...(payload.env ?? {})},
    };
    const instance = payload.instance ?? this.cfg.defaultInstance;
    if (instance) options.instance = instance;
    try {
      this.ctx.container.start(options);
    } catch (error) {
      await this.advance('failed', {message: `Container did not start: ${error.message}`});
      return {status: 200, body: await this.status()};
    }
    this.monitor();
    await this.keepAlive();
    const result = await this.advance('started');
    // A cancel request can arrive while the container starts.
    if (result.state === 'canceling') await this.stop();
    return {status: 202, body: await this.status()};
  }

  async stop() {
    // SIGTERM first: the Dagster run worker reports the run as canceled.
    this.ctx.container.signal(15);
    let timer;
    const exited = this.monitor().then(() => true);
    const timedOut = new Promise(resolve => { timer = setTimeout(() => resolve(false), this.cfg.stopTimeoutMs); });
    const stopped = await Promise.race([exited, timedOut]);
    clearTimeout(timer);
    if (!stopped) {
      await this.ctx.container.destroy('Dagster run canceled');
      await this.monitoring;
    }
  }

  async cancel() {
    const result = await this.advance('cancel');
    if (result.action !== 'stop') return {terminated: false, ...(await this.status())};
    if (!this.ctx.container.running) {
      await this.advance('success');
      return {terminated: true, ...(await this.status())};
    }
    try {
      await this.stop();
    } catch (error) {
      await this.advance('cancel_failed', {message: `Could not stop container: ${error.message}`});
      return {terminated: false, ...(await this.status())};
    }
    return {terminated: true, ...(await this.status())};
  }

  async fetch(request) {
    switch (request.method) {
      case 'GET':
        return Response.json(await this.status());
      case 'DELETE':
        return Response.json(await this.cancel());
      case 'POST': {
        let payload;
        try {
          payload = await request.json();
        } catch {
          return Response.json({error: 'Body must be JSON'}, {status: 400});
        }
        const {status, body} = await this.launch(payload);
        return Response.json(body, {status});
      }
      default:
        return new Response('Method not allowed', {status: 405});
    }
  }
}

const RUN_PATH = /^\/runs\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i;

function authorized(request, token) {
  const supplied = new TextEncoder().encode(request.headers.get('Authorization') ?? '');
  const expected = new TextEncoder().encode(`Bearer ${token}`);
  return supplied.length === expected.length && crypto.subtle.timingSafeEqual(supplied, expected);
}

export async function route(request, env) {
  if (!env.DISPATCH_TOKEN) return new Response('DISPATCH_TOKEN is not set', {status: 503});
  if (!authorized(request, env.DISPATCH_TOKEN)) return new Response('Unauthorized', {status: 401});
  const match = new URL(request.url).pathname.match(RUN_PATH);
  if (!match) return new Response('Not found', {status: 404});
  const runId = match[1].toLowerCase();
  return env.RUNS.get(env.RUNS.idFromName(runId)).fetch(request);
}
