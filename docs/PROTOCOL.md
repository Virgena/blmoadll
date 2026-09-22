# The eggshellmod wire protocol

This document is for people who write plugins, especially plugins in TypeScript. After reading it you can write a plugin that loads, answers calls, streams, exchanges events, reads and writes the terminal, and shuts down cleanly, without reading any Rust. The other side, a host that runs the kernel as a child process and writes no plugins, is section 14.

What the kernel knows: capability ids, semantic versions, and which process to talk to. It does not know what a model, a session, a tool or a UI is, and it does not care what language your plugin is written in. The only interface is the framing below.

Conventions: "must" marks something the kernel enforces; "should" marks something the kernel does not check but that still misbehaves when you get it wrong (timeouts, orphaned chunks).

---

## 0. At a glance

The kernel starts your plugin as a child process using `command` / `args` from the config file, then speaks JSON-RPC on its fd 0 / fd 1:

```
the child process the kernel started
  fd 0 (stdin)   <- frames the kernel sends you
  fd 1 (stdout)  -> frames you send to the kernel
  fd 2 (stderr)  -> logs, not protocol; forwarded line by line to the kernel's stderr
```

A frame looks like this (header block + blank line + UTF-8 JSON body):

```
Content-Length: 78\r\n
\r\n
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocol":1}}
```

One complete round:

```
kernel -> you   initialize   you reply {protocol, provides, requires}
kernel -> you   start        you reply {}
kernel -> you   invoke       you reply with a result (or {stream_id})   <- your actual work
                both ways: $/stream/chunk, $/event, $/io/data
kernel -> you   shutdown     you reply {} and exit
```

Three traps worth memorising first:

1. **Only frames may appear on stdout.** Any `console.log` / `printf` debugging breaks the protocol. Write logs to stderr.
2. **The process must not die early.** Exiting on your own after `initialize` and before `shutdown` is a crash: the kernel marks that capability slot unavailable.
3. **A streamed reply sends `{stream_id}` first, then its chunks.** In the other order the chunks are orphaned and dropped.

---

## 1. Transport and framing

LSP-style framing; only `Content-Length` means anything.

Rules (this is what the kernel's `read_frame` / `write_frame` actually do):

- Headers are `key: value` lines ending in `\n`; a trailing `\r` and padding spaces are tolerated. The header block ends at a blank line.
- Only `Content-Length` is interpreted; every other header is ignored. Names are compared case-insensitively.
- Values are trimmed; anything that does not parse as a number is a bad frame.
- **The header block is capped at 8 KiB** (all header lines together), and a single line is capped at 8 KiB too. Over that is a bad frame.
- The body is `Content-Length` bytes of UTF-8 JSON.
- **The body is capped at `max_frame_bytes`, 64 MiB by default.** Over the cap is error code -32016: the kernel does not reply to you, it declares that pipe dead, logs a warning and kills the plugin. In other words -32016 never shows up as a response, only in logs, events and `--check`.
- The body must be a JSON **object**. Valid JSON that is not an object (an array, say) is -32600; not JSON at all is -32700.
- A header block with no `Content-Length` is a bad frame and kills the pipe.
- Frames have no separator and may arrive back to back. Every frame must be flushed (the kernel flushes each one).
- A clean EOF on fd 0 **at a frame boundary** means the peer closed: that is the plugin's normal exit path, exit code 0. EOF halfway through a header block is an error.
- The kernel reads frames, not lines; your input is not guaranteed to deliver exactly one frame per `read`, so you must buffer and reassemble frames yourself.

Writing (TypeScript, Node):

```ts
import { writeSync } from "node:fs";

function send(message: unknown): void {
  const body = Buffer.from(JSON.stringify(message), "utf8");
  writeSync(1, `Content-Length: ${body.length}\r\n\r\n`);
  writeSync(1, body);
}
```

Reading (the same loop reassembles frames):

```ts
let buffer = Buffer.alloc(0);

process.stdin.on("data", (chunk: Buffer) => {
  buffer = Buffer.concat([buffer, chunk]);
  for (;;) {
    const split = buffer.indexOf("\r\n\r\n");
    if (split < 0) return;                       // no complete header block yet
    const match = /content-length:\s*(\d+)/i.exec(
      buffer.subarray(0, split).toString("latin1"),
    );
    if (!match) throw new Error("missing Content-Length");
    const length = Number(match[1]);
    if (buffer.length < split + 4 + length) return; // no complete body yet
    const payload = buffer.subarray(split + 4, split + 4 + length);
    buffer = buffer.subarray(split + 4 + length);
    handle(JSON.parse(payload.toString("utf8")));
  }
});
```

Note that `Content-Length` counts **bytes**, not characters: CJK text and emoji must be measured with `Buffer.byteLength` / `Buffer.from(...).length`. `String.length` makes the kernel read half a character.

---

## 2. The JSON-RPC 2.0 envelope

Four envelopes:

```json
{"jsonrpc":"2.0","id":7,"method":"invoke","params":{...}}           request
{"jsonrpc":"2.0","method":"$/event","params":{...}}                 notification (no id, or a null id)
{"jsonrpc":"2.0","id":7,"result":{...}}                             success
{"jsonrpc":"2.0","id":7,"error":{"code":-32010,"message":"..."}}    failure
```

The order in which the kernel classifies a body (copied from `parse_frame`):

1. It must be a JSON object, else -32700 / -32600.
2. An `error` member makes it an error response; a missing `id` counts as null.
3. A `method` member with a non-null `id` is a request; a `method` with no `id` or a null `id` is a notification.
4. No `method` but an `id` is a success response; a missing `result` counts as null.
5. Neither `method` nor `id` is -32600.

About ids:

- The ids the kernel sends you are **numbers**, increasing from 1 on your pipe. Your reply must carry the same id back.
- Ids you send the kernel may be numbers or strings; the kernel echoes them unchanged. A string id such as `"r-1"` is perfectly legal.
- The two id spaces are independent; do not mix them.

`params` and `result` are opaque to the kernel: apart from the few members it reads itself (`capability` / `method` / `params` / `meta`), everything is forwarded unchanged.

---

## 3. The kernel to the plugin

The kernel sends only four requests: `initialize` / `start` / `invoke` / `shutdown`. Everything else is a notification.

### 3.1 initialize (request, must be answered)

The kernel sends it as soon as the process is up. params:

```json
{
  "protocol": 1,
  "plugin_id": "provider",
  "kernel_version": "0.1.0",
  "config": {"prefix": "echo: "}
}
```

| field | meaning |
|---|---|
| `protocol` | the kernel's protocol version, currently 1 |
| `plugin_id` | the id you have in the config file |
| `kernel_version` | the version string of the kernel crate, for logs and diagnostics only |
| `config` | the verbatim JSON of the `[plugins.<id>.config]` table in the config file; `{}` when absent |

This section is your "startup parameters". What goes in the config is entirely an agreement between you and whoever writes the config; the kernel does not look at it.

It genuinely does not: the table is opaque JSON and its keys are not validated. **Which keys to read is the plugin's business**: in plugin-kit that declaration is `Definition.configKeys`, and on initialize it records one warn per extra key in `config` following §4.4 (one is enough to report, and it only reports: the config may be newer than the plugin, which is an upgrade order, not an error). Declaring no `configKeys` skips the check entirely.

You must reply within `initialize_timeout_ms` (default 5000, overridable under `[plugins.<id>]`). A timeout means the startup failed.

result must contain:

```json
{
  "protocol": 1,
  "provides": [{"capability": "demo.text", "version": "1.0.0"}],
  "requires": [{"capability": "demo.tools", "version": "^1", "optional": true}]
}
```

| field | required | notes |
|---|---|---|
| `protocol` | yes | must equal 1. Anything else is -32015, the kernel startup fails, and it says "you claim protocol N, the kernel says 1" |
| `provides` | yes | array. Every entry needs `capability` and `version` |
| `requires` | yes | array. Every entry needs `capability` and `version`; `optional` defaults to false |

- `provides[].version` must be a **complete semver** (`1.0.0`), because the kernel uses it to satisfy other plugins' ranges.
- `requires[].version` must be a **semver range** (`^1`, `>=1.2, <2`).
- An invalid version string is a startup error, not a warning.
- Missing either `provides` or `requires` is an error (the messages are "initialize reply has no `provides` array" / "no `requires` array").
- A `optional: true` dependency with no provider only logs a warning and is skipped (it does not block startup); a required dependency that is missing, whose version does not match, or that forms a cycle fails startup.
- Other fields in result are ignored. The kernel builds the routing table and the topological order from `provides` / `requires` alone.

The dependency semantics are worth stating plainly: `requires` says "I need another capability to exist at a matching version". It decides **startup order** (your start is called after the start of what you depend on) and does not prevent you from calling any capability at runtime; runtime calls are bounded by the routing table only.

### 3.2 start (request, must be answered)

Called in topological order once every dependency is ready. params:

```json
{"capabilities": {"demo.text": {"plugin": "provider", "version": "1.0.0"}}}
```

- This is a **snapshot of the whole routing table**, sorted by capability id. It is a starting point, not a promise: later changes arrive as `kernel.capabilities.changed` events.
- This is the right place to prepare for "using someone else's capability"; the `start` of what you depend on has already returned.
- Timeout `start_timeout_ms` (default 10000).
- The result is ignored; reply `{}`.
- After `start` succeeds the kernel publishes `kernel.plugin.started`.
- `--check` (config check) does **not** call `start` and produces no lifecycle events at all.

### 3.3 invoke (request, must be answered): your business entry point

params:

```json
{
  "capability": "demo.text",
  "method": "chat",
  "params": {"prompt": "hi"},
  "meta": {
    "caller": "host",
    "request_id": 12,
    "timeout_ms": 30000,
    "stream": false
  }
}
```

| meta field | notes |
|---|---|
| `caller` | caller label. Either the id of a plugin or `"host"` |
| `request_id` | the kernel's call number on this pipe, a **number**. It matches your `$/cancel` notifications |
| `timeout_ms` | the timeout the kernel already set for this call; after it the caller gets -32012 and you get `$/cancel` |
| `stream` | true means a streaming call, see section 7 |

`"host"` is a reserved label: it stands for the host embedding the kernel (an editor or an application). The host is a **pure caller**: it provides no capabilities, has no process, and cannot be a routing target. Defining `plugins.host` in the config file is rejected.

result is any JSON you like, returned to the caller unchanged. On a streaming call it must be `{"stream_id": "<a name you pick>"}`.

Mind the direction: here `meta.request_id` is a number (the kernel's numbering), while **when you call back into the kernel** `meta.request_id` must be a string (your own request id). That is not a typo, it is two independent directions (see 4.1).

### 3.4 shutdown (request, must be answered)

```json
{"reason": "kernel_exit"}
```

| reason | when |
|---|---|
| `kernel_exit` | the host is exiting / the kernel's stdin hit EOF |
| `reload` | a hot reload replaced you (or a failed reload rolls back) |
| `ui_quit` | the user or host asked to quit |
| `check` | the `--check` run is wrapping up |

Those four are all there is. A host can only pick `ui_quit` (the default) and `kernel_exit`; `reload` and `check` are the kernel's own words (see 14.2).

**You must behave identically for every reason.** They exist for logs and metrics, not so you can branch on them.

What to do: reply `{}`, then exit on your own. If you have not exited within `shutdown_grace_ms` (default 5000) you are killed and the kernel's exit code becomes 2. Do not pick up new work after replying.

### 3.5 Kernel-to-plugin notifications

| method | params | when |
|---|---|---|
| `$/event` | `{topic, seq, payload}` | a topic you subscribed to has an event |
| `$/stream/chunk` | `{stream_id, seq, data, done}` | a stream chunk for a call you made |
| `$/stream/error` | `{stream_id, code, message, data}` | your stream ended with an error |
| `$/cancel` | `{request_id}` | a call you made was cancelled |
| `$/io/data` | `{stream:"stdin", data, eof}` | you attached stdin |
| `$/io/detached` | `{stream:"stdin", reason:"taken_over"}` | someone else took stdin |

Notifications have no id and must not be answered; answering one makes the kernel treat it as a request for an unknown method.

---

---

## 4. The plugin to the kernel

| method | type | what it does |
|---|---|---|
| `kernel.invoke` | request | call someone else's capability |
| `kernel.publish` | request | publish an event |
| `kernel.subscribe` | request | subscribe to a topic |
| `kernel.unsubscribe` | request | cancel a subscription |
| `kernel.attach` | request | take stdin / stdout |
| `kernel.detach` | request | release stdin / stdout |
| `kernel.write` | request | write to the host's stdout |
| `kernel.shutdown` | request | ask the whole kernel to go down |
| `kernel.log` | notification | structured log (no reply) |
| `$/stream/chunk` | notification | you are the provider, push a chunk |
| `$/stream/error` | notification | you are the provider, the stream failed |
| `$/cancel` | notification | cancel a call you made |

Any method the kernel does not recognise gets -32601.

### 4.1 kernel.invoke (request)

```json
{
  "jsonrpc": "2.0",
  "id": "r-1",
  "method": "kernel.invoke",
  "params": {
    "capability": "demo.text",
    "method": "chat",
    "params": {"prompt": "hi"},
    "meta": {"request_id": "r-1", "stream": false, "timeout_ms": 30000}
  }
}
```

| meta field | required | notes |
|---|---|---|
| `request_id` | **yes** | must be a **string**. Missing or not a string is -32602. Without it you cannot cancel the call |
| `stream` | no | defaults to false. true means you want a stream |
| `timeout_ms` | no | defaults to the provider plugin's `request_timeout_ms` |

Convention: use the same value for `request_id` and this request's `id`; the kernel maps your `$/cancel` onto the other side's call through the `(caller, request_id)` key.

result: the provider's result. On a streaming call it is `{"stream_id": "f-N"}`: the kernel picks that name, not you (the direction is reversed only for streams you provide, see section 7).

Errors you can hit:

| code | when |
|---|---|
| -32010 | no plugin provides this capability id |
| -32011 | the provider has exited / is unavailable; `data` holds `{plugin, exit_code, signal}` |
| -32012 | timeout (`"`capability/method`" timed out"`) |
| -32014 | the provider is alive but has not `start`ed |
| -32019 | your concurrency limit or the global one is full |

### 4.2 kernel.publish (request)

```json
{"jsonrpc":"2.0","id":2,"method":"kernel.publish",
 "params":{"topic":"demo.turn","payload":{"n":1}}}
```

result is `{}`.

- `topic` must not start with `kernel.`: that prefix is reserved for the kernel, and a violation is -32602 with `error.data.reason` set to `"reserved_topic"`.
- `payload` must not serialise past `event_payload_bytes` (default 256 KiB), else -32020.
- Publishing is fire and forget: there is no delivery receipt and no count of subscribers.

### 4.3 kernel.subscribe / kernel.unsubscribe (requests)

```json
{"method":"kernel.subscribe","params":{"patterns":["demo.*","kernel.plugin.*"]}}
→ {"subscription_id":"sub-7"}
```

- `patterns` must not be an empty array, else -32602.
- One subscription may carry several patterns; any match delivers the event.
- To cancel:

```json
{"method":"kernel.unsubscribe","params":{"subscription_id":"sub-7"}}
→ {}
```

An unknown subscription id is -32021. **All of a plugin's subscriptions die with it**; the kernel clears them, so you neither have to nor get the chance to unsubscribe yourself.

### 4.4 kernel.log (notification, no reply)

```json
{"jsonrpc":"2.0","method":"kernel.log",
 "params":{"level":"info","message":"loaded 3 tools","fields":{"count":3}}}
```

| field | notes |
|---|---|
| `level` | `error` / `warn` / `info` / `debug`; anything else is treated as debug |
| `message` | human-readable string, defaults to the empty string |
| `fields` | any JSON object, defaults to `{}`; flattened into the log line |

The kernel adds the `plugin` field (your id) and writes the line to its own stderr (one JSON object per line, `target` set to `"plugin"`). A line past `log_line_bytes` (default 8 KiB) is dropped and a warning is recorded. Levels below the current verbosity are filtered out.

This is the **only** recommended logging channel. Writing plain text to stderr is forwarded too (the kernel reads your stderr line by line with `target` `"plugin.stderr"`, adds the `plugin` field, and truncates at `log_line_bytes`), but the structured channel is easier to search.

Once more: do not log to stdout, that is the protocol.

### 4.5 The io primitives (requests)

```json
{"method":"kernel.attach","params":{"stream":"stdin","mode":"line"}}  → {}
{"method":"kernel.detach","params":{"stream":"stdin"}}                → {}
{"method":"kernel.write","params":{"stream":"stdout","data":"..."}}   → {}
```

Details are in section 9. `stream` accepts only `"stdin"` and `"stdout"`; anything else is -32602.

### 4.6 kernel.shutdown (request)

Asks the whole kernel to go down. The kernel replies `{}` **first** and then starts the unified shutdown (reason recorded as `ui_quit`).

### 4.7 Plugin-to-kernel notifications

| method | params | notes |
|---|---|---|
| `$/stream/chunk` | `{stream_id, seq, data, done}` | you are the provider, push a chunk |
| `$/stream/error` | `{stream_id, code, message, data}` | you are the provider, the stream failed |
| `$/cancel` | `{stream_id}` or `{request_id}` | give up a call you made |

`$/cancel` takes one of the two fields: `stream_id` cancels a stream where you are the caller (the kernel cancels the provider along with it), `request_id` cancels a call you made with `kernel.invoke` (using the string id you wrote). Cancellation is **cooperative**: the kernel stops forwarding and sends `$/cancel` to the provider, but it cannot force the other side to stop working.

---

## 5. Lifecycle and exit

```
kernel                                        plugin process
 │  spawn(command, args, cwd, env)             │
 │ ──────────────────────────────────────────► │  up, starts reading fd 0
 │                                             │
 │  initialize {protocol,plugin_id,config} ──► │
 │ ◄──────────────── {protocol,provides,requires}
 │                                             │
 │  (validate the dependency graph + order it; │
 │   any error fails the whole startup)        │
 │                                             │
 │  start {capabilities} ────────────────────► │   in topological order, deps first
 │ ◄──────────────── {}                        │
 │                                             │
 │  invoke / $/event / $/stream/chunk  ◄─────► │   normal operation
 │                                             │
 │  shutdown {reason} ───────────────────────► │
 │ ◄──────────────── {}                        │
 │                                             │  exits on its own
 │  wait shutdown_grace_ms; else kill → exit 2 │
```

The order is fixed: `initialize` → `start` → work → `shutdown`.

- No `invoke` reaches you before `start` (the kernel sends no calls before the routing table exists).
- To stop on your own initiative, exit yourself, but read the next part first.

### A plugin exiting unexpectedly

Exiting after initialize but before the kernel asks you to shut down counts as a **crash**. What the kernel does:

1. Every call still waiting for your reply immediately gets -32011 with `error.data` `{plugin, exit_code, signal}`.
2. The streams where you are the provider are terminated; callers get a -32011 `$/stream/error`.
3. The streams where you are the caller are cleared.
4. It emits `kernel.plugin.degraded` and then `kernel.plugin.stopped` with `payload.reason` set to `"crash"`.
5. **Your capability slots stay.** Later calls to those capabilities still get -32011 rather than quietly landing on another plugin, deliberately: a silent reroute is worse than an error.

The kernel does not exit because a plugin died, and other plugins are unaffected.

### The kernel's exit codes

| code | meaning |
|---|---|
| 0 | clean: every plugin exited on its own within grace, and no call was left behind |
| 2 | a plugin was killed, or a call did not finish within the drain budget (`drain_ms`, default 5000) |
| 1 | could not come up: the config was unreadable, or readable but unbootable |

### The internal shutdown order (you may rely on it)

1. **drain**: wait for in-flight calls to finish, until there are zero in flight and things have been quiet for `io_eof_idle_ms` (default 500), or until `drain_ms` is up.
2. Send `shutdown{reason}` to every plugin.
3. Wait `shutdown_grace_ms` (default 5000, the largest value among plugins).
4. Kill whatever is left, then the kernel exits.

So: after `shutdown` you have about 5 seconds. Flush to disk and close connections now, not later.

---

## 6. Capability calls and routing

- A capability id is an **opaque string**. The kernel has no idea what `"demo.text"` means, only which plugin owns it and at what version. The names are an agreement between you and whoever writes the config.
- The routing table is derived from the plugins' `provides`: whatever you declare in `initialize` automatically has a slot in the config, so there is nothing to copy into the config file. Declared means callable; to retire a capability, drop the plugin providing it from the config.
- A plugin whose required capabilities nobody serves **does not start**. It is spawned and initialized, then held back in a waiting state: its `provides` are not in the table and its own capabilities answer -32010, until a reload brings a provider in (§8.6). Plugins that require what it provides wait in turn, and the kernel says who waits for what through `kernel.plugin.blocked` (§8.5). A plugin held back like that is still a plugin the kernel knows about: it keeps its process and its declaration, and it is started the moment the slot it waits for exists.
- One provider per capability id. No priorities, no failover, no load balancing; to change providers, change the config (or hot reload).
- A hot reload **replaces the whole table** (not single routes), and plugins get the new table through `kernel.capabilities.changed`. Which means: always call by capability id and never cache "which plugin" locally, or you will be pointing at the old world after a reload.
- The caller label is written by the kernel and the caller's own claim does not count: a plugin call carries the plugin id, a host call carries `"host"`. You cannot impersonate another caller.
- Concurrency: `max_inflight` per plugin (default 64) and `max_inflight_total` globally (default 1024). Past either you get -32019 straight away, **no queueing**, because the kernel would rather a caller learns it is crushing the other side than silently grows a backlog.
- The kernel does not retry. Retrying is the caller's decision.
- Failures are immediate only when the provider has exited or has not `start`ed yet (-32011 / -32014). A waiting plugin is not a provider at all: `invoke` on a capability it would have offered gets -32010 (unknown capability), because the slot is not in the table.

---

---

## 7. Streams

A call is either one question and one answer, or one stream. A stream is a **sequence of notifications on the same pipe**, not a new connection.

### 7.1 You as the caller

```json
{"id":"r-1","method":"kernel.invoke",
 "params":{"capability":"demo.text","method":"chat","params":{},
           "meta":{"request_id":"r-1","stream":true}}}
→ {"id":"r-1","result":{"stream_id":"f-3"}}
```

Then:

```json
{"method":"$/stream/chunk","params":{"stream_id":"f-3","seq":0,"data":{"delta":"c0"},"done":false}}
{"method":"$/stream/chunk","params":{"stream_id":"f-3","seq":1,"data":null,"done":true}}
```

The points that matter:

- `stream_id` is **a name the kernel made up** (`f-N`), not yours. Get it first, then wait for chunks.
- **`seq` is renumbered by the kernel from 0**; the provider's own seq does not travel. You only consume in increasing seq and read `done` for the end.
- There is exactly one terminal chunk: either `done:true` (with `data` necessarily null) or a `$/stream/error{stream_id, code, message, data}`.
- You can also give up early: send `$/cancel{stream_id: "f-3"}`.

### 7.2 You as the provider

When an `invoke` arrives with `meta.stream` true:

1. **Reply first** with `{"stream_id": "<your name>"}`. This step **cannot be skipped or deferred**: the kernel builds the `(you, your stream_id)` to caller-stream mapping from that reply. Chunks sent before it are discarded as orphans (the kernel records a "sent a chunk for unknown stream" warning).
2. Then send any number of chunks:

```json
{"method":"$/stream/chunk",
 "params":{"stream_id":"s-1","seq":0,"data":{"delta":"hel"},"done":false}}
```

3. Finish with `{"stream_id":"s-1","seq":1,"data":null,"done":true}`. **`data` must be null**: a terminal chunk carrying a payload is recorded as a warning and that data is dropped.
4. Or end with an error: `{"stream_id":"s-1","code":-32603,"message":"boom","data":{}}`.

About your own `seq`: the kernel **does not use** it (callers see the kernel's renumbering). Writing an increasing integer is still right, because it is what you debug your own problems with.

About the name: your `stream_id` only has to be unique **on your own pipe**; the kernel handles renaming across plugins. You may have several streams open at once.

### 7.3 What the kernel does for you

- Renaming: your `stream_id` becomes the caller's `f-N`.
- Renumbering: the caller sees `seq` increasing from 0.
- Backpressure: see below.
- Idle timeout: see below.
- Cancellation forwarding, single terminal chunk, and a -32011 delivered as `$/stream/error` when the provider crashes.

### 7.4 Backpressure (you need to know, because it bites)

- When a caller's outbound queue passes `queue_high_water_bytes` (default 2 MiB), chunks stop going straight out and are held inside the kernel instead.
- They are released only once the queue drops below `queue_low_water_bytes` (default 1 MiB). The sweep runs about every 100 ms, so this is not zero-latency.
- Holding has hard ceilings: `stream_buffer_chunks` (default 4096 chunks) and `stream_buffer_bytes` (default 8 MiB). **Past either, the stream is terminated** and the caller gets a -32019 `$/stream/error`.
- A slow reader (a queue so full that even the holding area has no room) is also terminated with -32019.
- The host's queue (`"host"`) is bound by exactly the same watermarks: an embedder that reads slowly is throttled the same way.

The corollary: **you cannot assume the other side receives as much as you push.** A long fast producer must either accept termination or implement a paged or pull-style interface of its own.

### 7.5 Idle timeout

A stream with no chunk for longer than `stream_idle_timeout_ms` (default 30000, overridable per **provider plugin**) is terminated: the caller gets -32012 ("stream went idle") and you get `$/cancel`.

So a long task needs **heartbeats**: send a chunk on a schedule, even an empty one, or you are treated as stuck.

### 7.6 Cancellation

- You cancelling a call you made: `$/cancel{request_id: "r-1"}` (the string id you wrote), or `$/cancel{stream_id: "f-3"}`.
- The kernel cancelling you: `$/cancel{request_id: <number>}` (the call number the kernel gave your provider).
- **Cancellation is cooperative.** The kernel only does two things: stop forwarding and pass `$/cancel` to the provider. It cannot force the provider to stop. On `$/cancel`, wrap up as soon as you can and stop sending chunks (chunks sent on a dead stream are dropped and logged at debug level).
- **There is no terminal frame after a cancellation**: neither `done:true` nor `$/stream/error` arrives. Whoever asked for the cancellation knows it gave up, and does not need the kernel to say so twice.
- The host can cancel its own streams too, with the same notification and the same params (see 14.2).

### 7.7 The id namespaces

| prefix | who makes it | used for |
|---|---|---|
| `f-N` | the kernel | a caller's stream_id |
| `sub-N` | the kernel | a subscription id |
| `host-N` | the kernel | a call the host made (visible on the host side only) |

A plugin's own stream_id is entirely free; it only has to be unique on its own pipe. Do not imitate the prefixes above.

---

## 8. Events

Events are a **broadcast**: what you publish goes to every subscriber whose pattern matches (the host included), and what you subscribe to may come from anyone.

### 8.1 Receiving an event

```json
{"method":"$/event",
 "params":{"topic":"kernel.plugin.started","seq":12,"payload":{"plugin":"p","pid":4242}}}
```

### 8.2 Pattern matching

Patterns are split on `.` and compared segment by segment:

- `*` matches exactly **one segment**.
- `**` is reserved and **matches nothing in v1** (do not count on it for multi-segment wildcards).
- A pattern and a topic must have the same number of segments.

| pattern | matches | does not match |
|---|---|---|
| `demo.*` | `demo.turn` | `demo.turn.started` (different segment count), `loops.turn` |
| `demo.turn` | `demo.turn` | `demo.turn.started` |
| `*.*` | `demo.turn` | `demo` |
| `kernel.plugin.*` | `kernel.plugin.started` | `kernel.plugin.a.b` |

### 8.3 seq

`seq` is a **bus-wide** increasing counter, not a per-topic one. It helps you notice "I missed something", but it cannot tell you whether one topic had a gap.

### 8.4 Delivery budget and drops

Every subscription has a budget, counted in delivered-but-unacknowledged items and bytes:

| item | default |
|---|---|
| `event_queue_len` | 1024 events |
| `event_queue_bytes` | 4 MiB |

Past budget:

1. **The newest event is dropped** (not the oldest).
2. You get a `kernel.event.dropped` with `payload` `{subscription_id, dropped_count}` and `seq` fixed at 0.
3. That drop notice **does not consume budget** and is sent at most once per second per subscription. It is the only way you find out you fell behind.
4. Budget is returned as soon as you consume (the kernel confirms delivery), so a stuck subscriber keeps triggering drops.

A frame's size is estimated as `payload bytes + topic length + 64`.

**Conclusion: events are best-effort, not a reliable queue.** Falling behind is reported explicitly but nothing is resent. Use capability calls when you need reliable delivery.

One exception is `replay` in §14.2: when you have just subscribed, you can ask the kernel to send one `kernel.plugin.started` for each plugin **currently alive**. The startup-time events happened before you had a subscription, and without a replay you would never see them, leaving the host no way to learn where each plugin's entry point is.

### 8.5 Events the kernel publishes itself

| topic | payload | when |
|---|---|---|
| `kernel.plugin.started` | `plugin`, `pid`, `trigger`, `cwd`, `command`, `args` | after a plugin's `start` succeeds |
| `kernel.plugin.stopped` | `plugin`, `reason` (`shutdown` or `crash`), plus `trigger` when the kernel asked for the stop | the process ended |
| `kernel.plugin.degraded` | `plugin`, `code`, `signal`, `message` | a plugin exited without being asked |
| `kernel.capabilities.changed` | `capabilities` (the whole new table) | a hot reload swapped the routing table |
| `kernel.config.reloaded` | `path` | a config change was accepted |
| `kernel.event.dropped` | `subscription_id`, `dropped_count` | you fell behind |

These topics use the `kernel.` prefix and plugins may not publish them. To receive them, `kernel.subscribe` first, for example `{"patterns":["kernel.plugin.*"]}`.

`trigger` says what caused this lifecycle change:

| trigger | meaning |
|---|---|
| `boot` | brought up, or held back, by the kernel at startup |
| `config` | the config file changed and the reloader brought it up, stopped it or left it waiting |
| `source` | the host saw source change and restarted this plugin by name (§8.6) |
| `manual` | the host restarted it by hand |

When a host `subscribe` carries `replay: true`, the kernel sends one `kernel.plugin.started` for each plugin alive right now, with exactly the fields a live event has and `trigger` set to the value from when they came up, plus one `kernel.plugin.blocked` for each plugin waiting right now. These are real events: a plugin subscribed to a matching pattern receives them as well.

`kernel.plugin.started` also carries `cwd`, `command` and `args`, the values after loader resolution. They are how a host knows where a plugin's entry file lives, for example to map a changed file back to the plugin that should restart. A crash (`reason: "crash"`) has no `trigger`; only a stop the kernel itself asked for has one.

### 8.6 Hot reload and events

When a config file change is accepted (at any layer, see §12.4), the kernel emits `kernel.capabilities.changed` (with the whole new table) and `kernel.config.reloaded`. **The kernel itself does not restart** during a reload: added or changed plugins are brought up and go through `initialize` + `start` again, while replaced plugins are drained and then get `shutdown{reason:"reload"}`. A failed reload (the new config is broken) is rolled back as a whole and the old config keeps running; you only see a log line, no event.

A reload can also leave a plugin without something it requires: if the provider is gone (disabled, or no longer declaring the slot), the consumer is stopped (its `kernel.plugin.stopped` carries `trigger: config`) and enters the waiting state of §6, and the reload that brings the provider back starts the consumer again. A `restart` whose plugin lands in the waiting state still answers `{}`; `kernel.plugin.blocked` is what says what it is waiting for. A restart **stops first, then starts**: the old instance goes down cleanly through `shutdown{reason:"reload"}` and is waited for. Past that plugin's `shutdown_grace_ms` it is killed, and if it still refuses to leave the restart is rejected with -32011. Then the same bring-up pipeline a reload uses runs: spawn, `initialize`, validate the whole graph, swap the table, `start`, and finally another `kernel.capabilities.changed`. Stopping first is because things like ports exist once: the web plugin binds 8341 by default, and starting the new instance first would only hit EADDRINUSE.

After a successful restart the new instance has a new pid, and the `trigger` on both `kernel.plugin.started` and `kernel.plugin.stopped` records who asked. The plugin itself still sees `reload`. If the new instance cannot come up, that plugin stays absent, capability calls get -32011 (`data.plugin` names it), the log carries one `restart rejected: ...` line, and another `restart` retries. A restart touches only the plugin named, so other plugins never even see the routing table move.

---

---

## 9. The terminal io primitives

Start with the boundary, so you do not think the kernel is quietly doing something else.

The kernel's slogan is "load, unload, dependencies, and nothing more". But **multiplexing the host's own stdin/stdout between plugins is transport work**, so the kernel counts it as its own job. This part genuinely goes past that slogan, and it is written down here honestly. Even so, the kernel still has no idea what a UI is: it only forwards bytes to one plugin at a time, by ownership.

### 9.1 stdin

- **One owner at a time.** `kernel.attach{stream:"stdin"}` is preemptive: the previous owner immediately gets `$/io/detached{stream:"stdin", reason:"taken_over"}`. The takeover happens inside one lock, so there is no window in which two plugins both believe they own stdin.
- **The kernel only starts reading its own stdin once someone attached.** Not one byte is read before the first attach, so input from `echo hi | ...` waits in the operating system's buffer instead of being lost.
- Data arrives as a string:

```json
{"method":"$/io/data","params":{"stream":"stdin","data":"hello\n","eof":false}}
```

- `data` is a **string**, not base64. Bytes that are not valid UTF-8 are **lossily decoded** (into U+FFFD), so use capability calls with base64 if you need binary.
- **Chunking follows read size, not lines** (`io_line_bytes`, default 8 KiB). The field name says line, but do not be misled: one `$/io/data` is not one line. Buffer it yourself if you want lines.
- When the host's stdin ends you first get `{"data":"","eof":true}`, and then the kernel begins its unified shutdown (reason = `kernel_exit`). That is the canonical "the upstream pipe closed" path.
- A read is interrupted by detach; after re-attaching you may lose at most one chunk that was read but not yet delivered.
- `mode` (default `"line"`) is **recorded only and changes no behaviour** in v1. Do not depend on it.

### 9.2 stdout

- `kernel.attach{stream:"stdout"}` claims ownership. The stdout `mode` has no effect in v1.
- Writing:

```json
{"method":"kernel.write","params":{"stream":"stdout","data":"\u001b[2J"}}
→ {}
```

  - Only `stream: "stdout"` is allowed (anything else is -32602).
  - A single `data` past `event_payload_bytes` (default 256 KiB) is -32020.
  - A backlog in the kernel's stdout queue past `io_write_queue_bytes` (default 4 MiB) is -32019. Do not treat stdout as bottomless.
  - `data` is a string, and the raw bytes written are its UTF-8 encoding. Terminal escape sequences can be written directly.
- `kernel.detach{stream:"stdout"}` releases it.
- **Detach is explicitly not a shutdown signal.** Releasing stdout does not trigger going down.

---

## 10. Error code table

The five standard JSON-RPC codes are reused verbatim; the rest are eggshellmod's own block.

| code | name | meaning |
|---|---|---|
| -32700 | `parse_error` | the frame body is not valid JSON |
| -32600 | `invalid_request` | valid JSON but not an object, or neither `method` nor `id` |
| -32601 | `method_not_found` | the kernel does not know this method |
| -32602 | `invalid_params` | params missing or invalid |
| -32603 | `internal_error` | an error inside the kernel (a response that was not delivered, for instance) |
| -32010 | `unknown_capability` | no plugin provides this capability id |
| -32011 | `provider_unavailable` | the provider has exited or is unavailable |
| -32012 | `request_timeout` | the call timed out, or a stream went idle |
| -32013 | `cancelled` | cancelled |
| -32014 | `not_started` | the provider is alive but has not `start`ed |
| -32015 | `protocol_version_mismatch` | the `protocol` in `initialize` is not 1 |
| -32016 | `frame_too_large` | the frame is past `max_frame_bytes` |
| -32017 | `unknown_stream` | unknown stream id |
| -32018 | `invalid_config` | a config or startup problem (a failed spawn, for instance) |
| -32019 | `overloaded` | concurrency, a queue, or a stream buffer is full |
| -32020 | `payload_too_large` | the payload is past its ceiling (an event or `kernel.write`) |
| -32021 | `unknown_subscription` | unknown subscription id |

-32602 shows up in these places: a `kernel.invoke` missing the string `meta.request_id`, an `initialize` reply missing `provides`/`requires`, a version string that is not valid semver, an empty subscription `patterns`, an io `stream` that is neither `stdin` nor `stdout`, a `kernel.write` whose `stream` is not `stdout`, an ill-formed `kernel.unsubscribe` id, a `kernel.publish` using the `kernel.` prefix, and a host `shutdown` whose `reason` is not one of the two allowed values.

Three notes:

- **-32016 is never a response.** Once a frame is too large the pipe is no longer trustworthy, so the kernel logs it (as a log line, an event, or `--check` output) and kills the plugin.
- `-32013` (cancelled) and `-32017` (unknown_stream) are currently **reserved values** in the code: they have names in the table but no normal path produces them. Write plugins as if they might appear, but do not rely on seeing them.
- Under the kernel's `--check` mode `-32011` means something special: the plugin exited on its own after `initialize`. In normal operation `-32011` means "the provider is gone".

---

## 11. Limits and timeouts

Every limit lives in the config file's `[kernel]` table under the same field name. All have defaults, so write only what you change.

| field | default | what it does |
|---|---|---|
| `max_frame_bytes` | 64 MiB | frame body ceiling, past it -32016 |
| `initialize_timeout_ms` | 5000 | how long the kernel waits for the `initialize` reply |
| `start_timeout_ms` | 10000 | how long it waits for the `start` reply |
| `shutdown_grace_ms` | 5000 | how long it waits for you to exit after `shutdown` |
| `request_timeout_ms` | 30000 | timeout of one ordinary capability call |
| `stream_idle_timeout_ms` | 30000 | how long a stream may go without a chunk |
| `event_payload_bytes` | 256 KiB | `kernel.publish` payload ceiling (also the single-write ceiling for `kernel.write`) |
| `event_queue_len` | 1024 | item budget per subscription |
| `event_queue_bytes` | 4 MiB | byte budget per subscription |
| `outbound_queue_bytes` | 4 MiB | outbound queue ceiling towards one plugin |
| `io_write_queue_bytes` | 4 MiB | ceiling of the kernel's stdout queue |
| `queue_high_water_bytes` | 2 MiB | upper watermark; past it stream chunks are held |
| `queue_low_water_bytes` | 1 MiB | lower watermark; chunks are released once it is reached |
| `stream_buffer_chunks` | 4096 | held-chunk ceiling for one stream |
| `stream_buffer_bytes` | 8 MiB | held-byte ceiling for one stream |
| `max_inflight` | 64 | in-flight calls per plugin |
| `max_inflight_total` | 1024 | in-flight calls overall |
| `drain_ms` | 5000 | budget for waiting on in-flight calls at shutdown |
| `io_eof_idle_ms` | 500 | how quiet things must be at shutdown to count as done |
| `max_plugins` | 64 | plugin count ceiling |
| `log_line_bytes` | 8 KiB | log line truncation, and the ceiling for one plugin stderr line |
| `io_line_bytes` | 8 KiB | read size per stdin read |

A plugin may override these under `[plugins.<id>]`:

| field | overrides |
|---|---|
| `initialize_timeout_ms` | the `initialize` timeout |
| `start_timeout_ms` | the `start` timeout |
| `shutdown_grace_ms` | the exit grace period |
| `request_timeout_ms` | the default timeout when someone calls this plugin |
| `stream_idle_timeout_ms` | the idle timeout of the streams you provide |
| `max_inflight` | how many calls others may have in flight against you |

---

---

## 12. Checklist and a minimal plugin

### 12.1 How the kernel brings you up

A config fragment:

```toml
[plugins.demo]
command = "node"                    # a bare name goes to the OS to be found on PATH
args = ["plugins/minimal-plugin.js"]
# name = "@scope/plugin"          # the other form: run a package instead of a program
# cwd defaults to the directory of the entry config file (see §12.4)
# env = { API_KEY = "${TOKEN}" }    # ${VAR} expands; an unset variable is a hard error
# clear_env = true                  # drop the inherited environment
# disabled = true                   # the row stays but nothing starts; an upper layer writing disabled = false enables it
# request_timeout_ms = 1000         # the default timeout when someone calls me

[plugins.demo.config]               # appears verbatim as initialize's params.config
greeting = "hi"

# [capability]                     # only needed when two plugins fight over one capability
# "demo.text" = "demo"             # that one pin: who provides this capability (see §6)
```

- A `command` **containing a path separator** is resolved against the entry config file's directory; a **bare name** goes to PATH (so `node`, `python` and `deno` can be written directly).
- `args` also expands `${VAR}` but is not treated as a path.
- A row names either a `command` or a `name`, never both, and one of the two is required.
- `name` is resolved the way Node resolves an import from the config file: the nearest `node_modules/<name>/package.json` at or above the entry config file's directory. The entry is that manifest's own `exports["."]` (`import`, then `default`, then the export itself when it is a string) or its `main`, defaulting to `index.js`, and the file is run with `node`. The row's `args` are appended after the entry. The resolved file is the real one on disk, so a link in a profile directory still runs the package it points at.
- An undefined `${VAR}` is a **hard error** (refusing to start beats silently becoming an empty string); `$$` is a literal `$`; expansion is **not recursive**.
- A plugin id may not be `host` (reserved for the host).
- A row with `disabled = true` still takes part in layer merging, but the kernel does not start it and it is not in the capability table. An upper layer overriding it to `false` enables it, and the reloader brings it up on its own once it sees the file change (no kernel restart needed).

### 12.2 A minimal plugin that runs (TypeScript / Node)

```ts
#!/usr/bin/env node
// An eggshell plugin written with Node: speaking the protocol is all it takes.
import { writeSync } from "node:fs";

const provides = [{ capability: "demo.text", version: "1.0.0" }];
const requires: { capability: string; version: string; optional?: boolean }[] = [];

function send(message: unknown): void {
  const body = Buffer.from(JSON.stringify(message), "utf8");
  writeSync(1, `Content-Length: ${body.length}\r\n\r\n`);
  writeSync(1, body);
}

function chunk(streamId: string, seq: number, data: unknown, done: boolean): void {
  send({ jsonrpc: "2.0", method: "$/stream/chunk",
         params: { stream_id: streamId, seq, data, done } });
}

let buffer = Buffer.alloc(0);

process.stdin.on("data", (incoming: Buffer) => {
  buffer = Buffer.concat([buffer, incoming]);
  for (;;) {
    const split = buffer.indexOf("\r\n\r\n");
    if (split < 0) return;
    const match = /content-length:\s*(\d+)/i.exec(
      buffer.subarray(0, split).toString("latin1"));
    if (!match) throw new Error("missing Content-Length");
    const length = Number(match[1]);
    if (buffer.length < split + 4 + length) return;
    const payload = buffer.subarray(split + 4, split + 4 + length);
    buffer = buffer.subarray(split + 4 + length);
    handle(JSON.parse(payload.toString("utf8")));
  }
});

// The kernel closed stdin (or asked for shutdown): leave cleanly.
process.stdin.on("end", () => process.exit(0));

function handle(message: any): void {
  const { id, method, params } = message;
  if (method === undefined) return;                 // a response; this sample calls nobody
  if (id === undefined || id === null) return;      // a notification; no reply wanted

  switch (method) {
    case "initialize":
      send({ jsonrpc: "2.0", id,
             result: { protocol: 1, provides, requires } });
      return;

    case "start":
      // params.capabilities is the whole routing table snapshot; cache it if you need it.
      send({ jsonrpc: "2.0", id, result: {} });
      return;

    case "invoke": {
      if (params?.meta?.stream !== true) {
        send({ jsonrpc: "2.0", id, result: { echo: params?.params ?? null } });
        return;
      }
      // Streaming: the stream_id reply must come first, then the chunks.
      const streamId = `s-${id}`;
      send({ jsonrpc: "2.0", id, result: { stream_id: streamId } });
      for (let n = 0; n < 3; n += 1) {
        chunk(streamId, n, { delta: `chunk ${n}` }, false);
      }
      chunk(streamId, 3, null, true);                // terminal chunk: data must be null
      return;
    }

    case "shutdown":
      // Every reason is treated the same: wrap up, reply, exit.
      send({ jsonrpc: "2.0", id, result: {} });
      process.exit(0);

    default:
      send({ jsonrpc: "2.0", id,
             error: { code: -32601, message: `no ${method} here` } });
  }
}

// To speak up on your own, just send, for example:
// send({ jsonrpc: "2.0", method: "kernel.log",
//        params: { level: "info", message: "ready", fields: {} } });
```

Run it:

```toml
[plugins.demo]
command = "node"
args = ["minimal-plugin.mjs"]
```

The stand-in plugin the kernel tests use (`crates/kernel/src/bin/eggshell-fixture.rs`, Rust) has this shape in another language; read it whenever something is unclear.

### 12.3 A self-check list

Go through this before shipping:

- No non-frame bytes on stdout (including a dependency's print, a progress bar, a banner).
- Every request was answered with the same `id` (numbers echoed back unchanged).
- `initialize` replied with `protocol: 1` and both arrays.
- In a streaming reply, `{stream_id}` goes out before the first chunk.
- The terminal chunk has `done:true` and `data: null`.
- Long tasks send heartbeat chunks, so they do not hit the 30 second idle timeout.
- All four `shutdown` reasons behave the same, and you really do exit.
- Logs go through `kernel.log` or stderr, never stdout.

### 12.4 Config files can be layered (`extends`)

A config file can list, with `extends`, the files it layers on top of:

```toml
extends = ["eggshell.base.toml", "team.toml"]   # loaded first, in this order
```

- **Every listed file must exist.** A missing one fails startup and names the file that could not be read, so a typo in a filename is visible immediately instead of degrading into "one layer short but looking fine".
- Relative paths are resolved against the directory of **the file that writes them**; absolute paths are used as written.
- Later layers win. Within a key the last writer counts; **tables merge key by key** (`[plugins.api.config]` overrides only the keys it mentions, the rest survive from the layer below) while **arrays and scalars are replaced whole**. So a layer may hold a single key.
- A relative `command` / `cwd` is resolved against the directory of **the file you passed at startup** (the entry layer), not the layer that declares it. When layers live in other directories, say so with an absolute path.
- A file reached twice (a diamond, or a loop back) counts once, at the position where it first appears. The layer stack is always finite and the order stable.
- `--check` prints which files this run is made of, base layers first and the entry last:

  ```
  config: eggshell.toml + eggshell.local.toml
  ```

- Hot reload watches **the whole stack**: editing a file named by the entry's `extends` triggers a reload exactly like editing the entry itself.
- `extends` is a file mechanism, not plugin config: it never appears in any plugin's `config`, and writing `extends` when loading from a string (with no file to resolve against) is a hard error. What a plugin sees is always the merged, resolved result.

---

## 13. Version and compatibility

- `PROTOCOL_VERSION` is currently 1.
- The version is negotiated through `initialize`'s `params.protocol` / `result.protocol`; a mismatch on either side is -32015, which fails startup and reports both versions explicitly.
- Any incompatible wire change bumps it. **Adding an optional field is not incompatible** (the kernel ignores fields it does not know), while removing a field or changing its semantics is.
- `kernel_version` is diagnostic only; do not gate features on it. Gate on `protocol`.

---

---

## 14. The host side: the kernel as a child process

The previous 13 sections take the "you write a plugin" point of view. This one is the other end: a **host** (MaoTa, written in TypeScript, for instance) does not link the kernel but spawns it as a child process and speaks the same framing on its fd 0 / fd 1.

Upward the kernel is just another plugin-shaped program: the host starts it and it starts the plugins. The layer below (the plugin protocol) is completely unaffected.

### 14.1 How to start it

```
eggshell <config.toml> [--check] [--json]
```

This executable is only produced by a `--features host` build. A default build produces no executables at all (apart from the test stand-in, see 12.2).

`--check` is a config check: it starts every plugin, runs `initialize` once, validates the capability graph and version ranges, computes the startup order, prints a report and exits (0 = pass, 1 = at least one error). A `disabled` row and a plugin left waiting are warnings, so a config that only has those still exits 0: the report carries them as `disabled` (plugin ids) and `blocked` (`{plugin id: [capability id]}`), and the text form prints one `disabled: a, b` line and one `waiting: a needs c` line per waiting plugin. It **never sends `start`**, never touches io and emits no lifecycle events, so it needs no network and has no side effects. Run it after editing a config; it is much faster than a real conversation. The report goes to stdout (under check mode that fd is not a protocol pipe) with the same fields as the one written to stderr on a failed startup, and `--json` prints the same report as one line of JSON instead of text.

```
host -> kernel fd 0    frames the host sends (requests)
host <- kernel fd 1    frames the kernel sends (replies + notifications)
        kernel fd 2    logs, one JSON object per line
```

Framing is exactly section 1 (`Content-Length`, 8 KiB header ceiling, UTF-8 JSON body). Plugins see no difference at all: `initialize` / `start` / `invoke` / `shutdown` are unchanged.

### 14.2 Methods a host may send

Exactly these six, anything else gets -32601:

| method | params | result |
|---|---|---|
| `invoke` | `capability`, `method`, `params`, optional `meta.stream` | the provider's business result; `{stream_id}` when streaming |
| `capabilities` | none | `{capability id: {plugin, version}}` |
| `subscribe` | `patterns` (array of strings), optional `replay` (boolean, default false) | `{subscription_id}` |
| `unsubscribe` | `subscription_id` | `{}` |
| `shutdown` | optional `reason` | `{}`, and going down starts **after this frame is sent** |
| `restart` | `plugin`, optional `reason` (`source` or `manual`, default `manual`) | `{}`; -32011 (`data.plugin`) when the new instance cannot come up, -32602 for an unknown plugin or reason |

One question, one answer:

```json
{"jsonrpc":"2.0","id":1,"method":"invoke",
 "params":{"capability":"demo.text","method":"echo","params":{"hi":1}}}
{"jsonrpc":"2.0","id":1,"result":{"got":{"hi":1}}}
```

Streaming is the same method plus `"meta":{"stream":true}`: the reply is `{"stream_id":"f-3"}`, chunks arrive as `$/stream/chunk` notifications and `seq` is still renumbered by the kernel (section 7). Events are the section 8 machinery with the host as the subscriber.

Only `stream` is read out of the host's `meta`. `timeout_ms` is a field for a plugin calling back into the kernel and is not passed through on the host side, so a host that wants a precise timeout has to time it itself.

Shutdown may carry a `reason`:

```json
{"jsonrpc":"2.0","id":9,"method":"shutdown","params":{"reason":"kernel_exit"}}
```

- Only two are ones a host can honestly give: `ui_quit` (the default when `reason` is omitted) and `kernel_exit`. `reload` / `check` or any other string gets -32602 and the kernel keeps running.
- What a plugin sees is always one of the four values in the table in 3.4.

`restart` is for development: the reason it stops with is still `reload`, and `reason` only decides the event's `trigger` (`source` = a file changed, `manual` = a person pressed it). The restart semantics are in §8.6.

Giving up a stream means sending one **notification**, with the same method and params as the plugin side (7.6):

```json
{"jsonrpc":"2.0","method":"$/cancel","params":{"stream_id":"f-3"}}
```

- **Cancellation is silent**: the kernel stops forwarding and passes `$/cancel` to the provider, but **no** terminal frame comes back, because it was your own decision. Do not wait for `done`.
- The id a host holds is the `stream_id` (`f-N`, right there in the reply). The `request_id` path needs the `host-N` number the kernel assigns to host calls (see 7.7) and is rarely useful.
- Cancelling an id that does not exist is a no-op: a repeated cancellation, or one on a stream that ended long ago, reports no error. So cancelling as the consumer `break`s is the standard move.

### 14.3 The host is a pure caller

- The kernel writes `meta.caller = "host"` for the host, so a provider can tell a host call from a plugin call.
- The host provides no capabilities, has no process and cannot be a routing target. There is nowhere in a config file that can write it as a provider (`plugins.host` is rejected, see 3.3).
- A host-initiated call is numbered `host-N` (see 7.7).
- The host is bound by the same in-flight ceiling as plugins, and going past it gets -32019 (overloaded, section 10).

### 14.4 Whose terminal it is

The kernel process's fd 0 / fd 1 are the host protocol and fd 2 is logs, so **under host mode the kernel has no terminal to give plugins**:

- A host cannot send the io primitives (`kernel.attach` / `kernel.detach` / `kernel.write`): all -32601.
- Neither can a plugin attach: the kernel refuses with -32601 ("the kernel runs as a host subprocess, so its terminal belongs to the host").
- For a plugin that wants to read the keyboard or write to the tty, v1 has no pass-through channel. That is a known gap, not a config problem.

The host's own stdin and Ctrl-C belong to the host process; the kernel never sees them.

### 14.5 Exit and reaping

- The host sends `shutdown` → the kernel replies `{}` → the unified shutdown runs (section 5) → it exits.
- The host **closes fd 0** (it stopped sending, or the host process died) → the kernel treats EOF as a shutdown signal with reason = `kernel_exit` and still exits cleanly. A host does not have to raise its hand and say goodbye.
- Exit codes: `0` clean; `2` some plugin was killed or a call did not drain; `1` could not come up, meaning the config was unreadable, or readable but unbootable.
- With exit code `1` there is always one JSON report line on stderr (`ok:false` plus `errors` / `warnings` / `plugins` / `capabilities`). It is mixed in with log lines; recognise it by the `ok` field.
- **Killing the kernel process directly, bypassing shutdown, leaves plugin child processes behind.** Reaping is part of the kernel's unified shutdown.

### 14.6 A minimal host (TypeScript)

This is the whole shape, no other magic: spawn a process, frame by `Content-Length`, send and receive. The bridge in MaoTa is this expanded over a few more lines.

```ts
import { spawn } from "node:child_process";

const kernel = spawn("eggshell", ["./eggshell.toml"], { stdio: ["pipe", "pipe", "inherit"] });

let buffer = Buffer.alloc(0);
kernel.stdout.on("data", (chunk) => {
  buffer = Buffer.concat([buffer, chunk]);
  for (;;) {
    const head = buffer.indexOf("\r\n\r\n");
    if (head < 0) return;
    const length = Number(/content-length:\s*(\d+)/i.exec(buffer.subarray(0, head).toString())?.[1]);
    if (!Number.isFinite(length) || buffer.length < head + 4 + length) return;
    const body = buffer.subarray(head + 4, head + 4 + length);
    buffer = buffer.subarray(head + 4 + length);
    onFrame(JSON.parse(body.toString("utf8")));
  }
});

function send(frame: unknown) {
  const body = Buffer.from(JSON.stringify(frame), "utf8");
  kernel.stdin.write(`Content-Length: ${body.length}\r\n\r\n`);
  kernel.stdin.write(body);
}

let nextId = 0;
const waiting = new Map<number, (reply: any) => void>();

function request(method: string, params: unknown): Promise<any> {
  const id = ++nextId;
  return new Promise((resolve) => {
    waiting.set(id, resolve);
    send({ jsonrpc: "2.0", id, method, params });
  });
}

function onFrame(frame: any) {
  if (frame.id !== undefined) waiting.get(frame.id)?.(frame); // a reply
  else dispatchNotification(frame);                          // $/event, $/stream/chunk
}
```

A few details worth copying exactly:

- fd 2 uses `inherit` to reach the host's own stderr, which is where kernel logs belong.
- One frame may span several `data` events, and several frames may arrive at once, so buffer until complete before parsing (the loop above).
- The kernel reads frames on fd 0 and writes them on fd 1 (symmetric with the plugin side), so do not let it inherit the host's own fd 0: `stdio: ["pipe", "pipe", "inherit"]`.
- Do not reuse the kernel after `shutdown`: the process exits and the host should boot a new one.
