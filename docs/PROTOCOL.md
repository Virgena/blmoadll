# eggshellmod 线协议

本文写给要写插件的人 —— 尤其是用 TypeScript 写插件的人。读完这一篇，你不需要看 Rust 代码就能写出一个能加载、能被调用、能推流、能收发事件、能读写终端、能被干净关掉的插件。反过来，宿主侧（把内核当子进程拉起来、自己不写插件的那一头）见第 14 节。

内核侧的事实基础: 内核只认能力 id、语义化版本和"该跟哪个进程说话"。它不知道 model / session / tool / UI 是什么，也不认你的插件是用什么语言写的。唯一的接口就是下面这套帧。

标记约定: 写「必须」的是内核会强制检查的; 写「应当」的是内核不检查但会因此行为异常（例如超时、孤儿块）的。

---

## 0. 速览

内核按配置文件里的 `command` / `args` 把你的插件当子进程拉起，然后在它的 fd 0 / fd 1 上讲 JSON-RPC:

```
内核拉起的子进程
  fd 0 (stdin)   <- 内核发给你的帧
  fd 1 (stdout)  -> 你发给内核的帧
  fd 2 (stderr)  -> 日志，不是协议；按行转发到内核的 stderr
```

一个帧长这样（头部 + 空行 + UTF-8 JSON 体）:

```
Content-Length: 78\r\n
\r\n
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocol":1}}
```

一个完整的回合:

```
内核 -> 你   initialize   你回 {protocol, provides, requires}
内核 -> 你   start        你回 {}
内核 -> 你   invoke       你回结果（或 {stream_id}）       <- 这里才是你的业务
             互相推 $/stream/chunk、$/event、$/io/data
内核 -> 你   shutdown     你回 {} 然后退出
```

三条最容易踩的坑，先记住:

1. **stdout 上只能出现帧。** 任何 `console.log` / `printf` 调试都会破坏协议。日志写 stderr。
2. **进程不能提前死。** 在 initialize 之后、shutdown 之前自己退出，等价于崩溃: 内核会把这个能力槽标成不可用。
3. **流式回复必须先回 `{stream_id}` 再发块。** 顺序反了，块会被当成孤儿丢掉。

---

## 1. 传输与分帧

采用 LSP 风格的分帧，只有 `Content-Length` 有意义。

规则（这些是内核 `read_frame` / `write_frame` 的实际行为）:

- 头部是 `键: 值` 行，以 `\n` 结束，行尾的 `\r` 和空格被容忍。头部块以空行结束。
- 只有 `Content-Length` 被解释；其他头部一律忽略。名字比较是不区分大小写的。
- 值的两侧空白被 trim；解析不出数字就是坏帧。
- **头部块上限 8 KiB**（所有头部行合计），单行也受 8 KiB 限制。超了算坏帧。
- 帧体是 `Content-Length` 个字节的 UTF-8 JSON。
- **帧体上限 `max_frame_bytes`，默认 64 MiB。** 超限是错误码 -32016: 内核不会回复你，而是判定这条管道作废、记一条 warning、把插件杀掉。也就是说 -32016 永远不会作为一条响应出现，只在日志 / 事件 / `--check` 里。
- 帧体必须是 JSON **对象**。是 JSON 但不是对象（比如数组）算 -32600; 不是 JSON 算 -32700。
- 头部块里没有 `Content-Length` = 坏帧，管道作废。
- 帧与帧之间没有分隔符，可以背靠背连发。一帧写完必须 flush（内核自己每帧 flush）。
- fd 0 在**帧边界处**读到干净 EOF 表示对端关闭 —— 这是插件的正常退出路径，退出码 0 即可。在头部读到一半时 EOF 是错误。
- 内核按帧读，不按行读；你的输入不保证一次 `read` 就是一帧，必须自己缓冲拼帧。

写（TypeScript，Node）:

```ts
import { writeSync } from "node:fs";

function send(message: unknown): void {
  const body = Buffer.from(JSON.stringify(message), "utf8");
  writeSync(1, `Content-Length: ${body.length}\r\n\r\n`);
  writeSync(1, body);
}
```

读（同一个循环负责拼帧）:

```ts
let buffer = Buffer.alloc(0);

process.stdin.on("data", (chunk: Buffer) => {
  buffer = Buffer.concat([buffer, chunk]);
  for (;;) {
    const split = buffer.indexOf("\r\n\r\n");
    if (split < 0) return;                       // 头部还没齐
    const match = /content-length:\s*(\d+)/i.exec(
      buffer.subarray(0, split).toString("latin1"),
    );
    if (!match) throw new Error("missing Content-Length");
    const length = Number(match[1]);
    if (buffer.length < split + 4 + length) return; // 体还没齐
    const payload = buffer.subarray(split + 4, split + 4 + length);
    buffer = buffer.subarray(split + 4 + length);
    handle(JSON.parse(payload.toString("utf8")));
  }
});
```

注意 `Content-Length` 计的是**字节数**，不是字符数 —— 中文和 emoji 必须用 `Buffer.byteLength` / `Buffer.from(...).length` 来算。用 `String.length` 会让内核读到半个字符。

---

## 2. JSON-RPC 2.0 信封

四种信封:

```json
{"jsonrpc":"2.0","id":7,"method":"invoke","params":{...}}          请求
{"jsonrpc":"2.0","method":"$/event","params":{...}}                通知（无 id，或 id 为 null）
{"jsonrpc":"2.0","id":7,"result":{...}}                            成功
{"jsonrpc":"2.0","id":7,"error":{"code":-32010,"message":"..."}}    失败
```

内核解析一个帧体的判定顺序（照抄 `parse_frame` 的实际逻辑）:

1. 必须是 JSON 对象，否则 -32700 / -32600。
2. 有 `error` 字段 → 当成错误响应，缺 `id` 就当 null。
3. 有 `method` 字段且 `id` 非 null → 请求; 有 `method` 但无 `id` 或 `id` 为 null → 通知。
4. 没有 `method` 但有 `id` → 成功响应; `result` 缺失当 null。
5. 既没有 `method` 也没有 `id` → -32600。

关于 id:

- 内核发给你的 id 是**数字**，在你这根管道上从 1 起单调递增。你回复时必须原样带回这个 id。
- 你发给内核的 id 可以是数字或字符串; 内核原样回填。`"r-1"` 这种字符串 id 完全合法。
- 两侧的 id 空间互相独立，不要混着用。

`params` 和 `result` 对内核是不透明的 —— 除了它自己读的那几个字段（`capability` / `method` / `params` / `meta` 等），其余内容原样转发。
---

## 3. 内核发给插件

内核只发四个请求: `initialize` / `start` / `invoke` / `shutdown`。其余都是通知。

### 3.1 initialize（请求，必须应答）

内核在进程起来后立刻发。params:

```json
{
  "protocol": 1,
  "plugin_id": "provider",
  "kernel_version": "0.1.0",
  "config": {"prefix": "echo: "}
}
```

| 字段 | 含义 |
|---|---|
| `protocol` | 内核的协议版本，当前是 1 |
| `plugin_id` | 你在配置文件里的 id |
| `kernel_version` | 内核 crate 的版本字符串，仅用于日志/诊断 |
| `config` | 配置文件里 `[plugins.<id>.config]` 那张表的原样 JSON; 没写就是 `{}` |

这一节的内容就是你的"启动参数"。配置里放什么完全由你和写配置的人约定，内核不看。

你必须在 `initialize_timeout_ms`（默认 5000，可在 `[plugins.<id>]` 里覆盖）内回复。超时 = 启动失败。

result 必须包含:

```json
{
  "protocol": 1,
  "provides": [{"capability": "demo.text", "version": "1.0.0"}],
  "requires": [{"capability": "demo.tools", "version": "^1", "optional": true}]
}
```

| 字段 | 必须 | 说明 |
|---|---|---|
| `protocol` | 是 | 必须等于 1。不等就是 -32015，内核启动失败，并说明"你说协议 N，内核说 1" |
| `provides` | 是 | 数组。每一项必须有 `capability` 和 `version` |
| `requires` | 是 | 数组。每一项必须有 `capability` 和 `version`，`optional` 缺省 false |

- `provides[].version` 必须是**完整 semver**（`1.0.0`），因为内核要用它去满足别人的 range。
- `requires[].version` 必须是 **semver range**（`^1`、`>=1.2, <2`）。
- 版本字符串不合法是启动错误，不是警告。
- `provides` / `requires` 缺任何一个都是错误（错误信息分别是 "initialize reply has no `provides` array" / "no `requires` array"）。
- `optional: true` 的依赖找不到提供方时只记警告并跳过（不会拦住启动）; 必需依赖找不到、版本不满足、或依赖成环都是启动失败。
- result 里其他字段被忽略。内核只用 `provides` / `requires` 建路由表和拓扑序。

依赖语义值得说清楚: `requires` 声明的是"我需要另一个能力存在且版本满足"，它决定**启动顺序**（你的 start 会在依赖的 start 之后被调用），并不阻止你在运行期调用任何能力 —— 运行期调用只受路由表约束。

### 3.2 start（请求，必须应答）

依赖全部就绪后按拓扑序调用。params:

```json
{"capabilities": {"demo.text": {"plugin": "provider", "version": "1.0.0"}}}
```

- 这是**整张路由表的快照**，按能力 id 排序。它是出发点，不是承诺: 之后的变化通过 `kernel.capabilities.changed` 事件到达。
- 在这里做"依赖别人的能力"的准备工作最合适 —— 依赖的 `start` 已经返回了。
- 超时 `start_timeout_ms`（默认 10000）。
- result 内容被忽略，回 `{}` 即可。
- `start` 成功后内核发布 `kernel.plugin.started`。
- `--check`（配置体检）**不会**调用 `start`，也不会产生任何生命周期事件。

### 3.3 invoke（请求，必须应答）—— 你的业务入口

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

| meta 字段 | 说明 |
|---|---|
| `caller` | 调用方标签。要么是某个插件的 id，要么是 `"host"` |
| `request_id` | 内核在这根管道上的调用号，是**数字**。它和你的 `$/cancel` 通知对应 |
| `timeout_ms` | 内核已经为这次调用设好的超时; 超时后调用方拿到 -32012，你会收到 `$/cancel` |
| `stream` | true 表示流式调用，见第 7 节 |

`"host"` 是保留标识: 它代表嵌入内核的那个宿主（编辑器 / 上层应用）。宿主是**纯调用方** —— 它不提供能力、没有进程、不能当路由目标。配置文件里定义 `plugins.host` 会被拒绝。

result 是你给的任意 JSON，原样回给调用方。流式调用时必须回 `{"stream_id": "<你自己起的名字>"}`。

注意方向: 这里 `meta.request_id` 是数字（内核编号），而**你反向调用内核时** `meta.request_id` 必须是字符串（你自己的请求 id）。这不是笔误，是两条独立的方向（见 4.1）。

### 3.4 shutdown（请求，必须应答）

```json
{"reason": "kernel_exit"}
```

| reason | 何时 |
|---|---|
| `kernel_exit` | 宿主要退出了 / 内核的 stdin 到了 EOF |
| `reload` | 热重载把你换掉了（或重载失败要回滚） |
| `ui_quit` | 用户/宿主主动要求退出 |
| `check` | `--check` 体检收尾 |

这四个是能给出的全部。宿主只能挑 `ui_quit`（默认）和 `kernel_exit`，`reload` 和 `check` 是内核自己的说法（见 14.2）。

**你必须对所有 reason 表现一致。** 它们只用于日志和指标，不是让你按 reason 分支的。

做法: 回复 `{}`，然后自己退出。`shutdown_grace_ms`（默认 5000）内没退出会被强杀，内核的退出码会变成 2。回复之后不要再去接新活。

### 3.5 内核发给插件的通知

| 方法 | params | 何时 |
|---|---|---|
| `$/event` | `{topic, seq, payload}` | 你订阅的主题有事件 |
| `$/stream/chunk` | `{stream_id, seq, data, done}` | 你当调用方时的流式块 |
| `$/stream/error` | `{stream_id, code, message, data}` | 你的流以错误结束 |
| `$/cancel` | `{request_id}` | 你发出的调用被取消 |
| `$/io/data` | `{stream:"stdin", data, eof}` | 你 attach 了 stdin |
| `$/io/detached` | `{stream:"stdin", reason:"taken_over"}` | 别人抢走了 stdin |

通知没有 id，不要回复; 回了内核会当成一个未知方法的请求处理。
---

## 4. 插件发给内核

| 方法 | 类型 | 作用 |
|---|---|---|
| `kernel.invoke` | 请求 | 调用别人的能力 |
| `kernel.publish` | 请求 | 发事件 |
| `kernel.subscribe` | 请求 | 订阅主题 |
| `kernel.unsubscribe` | 请求 | 取消订阅 |
| `kernel.attach` | 请求 | 取得 stdin / stdout |
| `kernel.detach` | 请求 | 释放 stdin / stdout |
| `kernel.write` | 请求 | 写宿主 stdout |
| `kernel.shutdown` | 请求 | 要求整机下线 |
| `kernel.log` | 通知 | 结构化日志（无回复） |
| `$/stream/chunk` | 通知 | 你是提供方，推一块数据 |
| `$/stream/error` | 通知 | 你是提供方，流以错误结束 |
| `$/cancel` | 通知 | 取消你发出的调用 |

内核不认识的方法一律回 -32601。

### 4.1 kernel.invoke（请求）

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

| meta 字段 | 必须 | 说明 |
|---|---|---|
| `request_id` | **是** | 必须是**字符串**。缺了或不是字符串 → -32602。没有它你就无法取消这次调用 |
| `stream` | 否 | 缺省 false。true 表示你要一条流 |
| `timeout_ms` | 否 | 缺省用提供方插件的 `request_timeout_ms` |

约定: `request_id` 建议和你这次请求的 `id` 用同一个值 —— 内核就是靠 `(调用方, request_id)` 这个键把你的 `$/cancel` 映射到对方的调用上的。

result: 提供方的结果。流式调用时是 `{"stream_id": "f-N"}` —— 这个名字是**内核起的**，不是你起的（方向反过来时才是你起名，见第 7 节）。

你会遇到的错误:

| code | 什么时候 |
|---|---|
| -32010 | 没有任何插件提供这个能力 id |
| -32011 | 提供方已退出 / 不可用; `data` 里有 `{plugin, exit_code, signal}` |
| -32012 | 超时（`"`capability/method`" timed out"`） |
| -32014 | 提供方还活着但没 `start` |
| -32019 | 你或全局的并发上限到顶 |

### 4.2 kernel.publish（请求）

```json
{"jsonrpc":"2.0","id":2,"method":"kernel.publish",
 "params":{"topic":"demo.turn","payload":{"n":1}}}
```

result 是 `{}`。

- `topic` 不能以 `kernel.` 开头 —— 那个前缀保留给内核，违规是 -32602，`error.data.reason` 为 `"reserved_topic"`。
- `payload` 序列化后不能超过 `event_payload_bytes`（默认 256 KiB），否则 -32020。
- 发布是"发射后不管": 没有投递回执，也不知道有几个订阅者。

### 4.3 kernel.subscribe / kernel.unsubscribe（请求）

```json
{"method":"kernel.subscribe","params":{"patterns":["demo.*","kernel.plugin.*"]}}
→ {"subscription_id":"sub-7"}
```

- `patterns` 不能是空数组，否则 -32602。
- 一个订阅可以有多个图案; 只要有一个匹配就会收到事件。
- 取消:

```json
{"method":"kernel.unsubscribe","params":{"subscription_id":"sub-7"}}
→ {}
```

未知的订阅 id 是 -32021。**插件退出时它的全部订阅自动失效**，内核会清掉，你不用（也来不及）自己去注销。

### 4.4 kernel.log（通知，无回复）

```json
{"jsonrpc":"2.0","method":"kernel.log",
 "params":{"level":"info","message":"loaded 3 tools","fields":{"count":3}}}
```

| 字段 | 说明 |
|---|---|
| `level` | `error` / `warn` / `info` / `debug`; 其他值按 debug 处理 |
| `message` | 人读的字符串，缺省空串 |
| `fields` | 任意 JSON 对象，缺省 `{}`; 会被摊平进日志行 |

内核会补上 `plugin` 字段（你的 id），把整行写进它自己的 stderr（一行一个 JSON 对象，`target` 为 `"plugin"`）。超过 `log_line_bytes`（默认 8 KiB）的行会被丢弃并记一条 warning。低于当前 verbosity 的级别会被过滤掉。

这是**唯一**推荐的日志通道。直接往 stderr 写原样文本也能被转发（内核按行读你的 stderr，`target` 为 `"plugin.stderr"`，带 `plugin` 字段，按 `log_line_bytes` 截断），但结构化通道更好检索。

再强调一次: 日志不要写 stdout，那是协议。

### 4.5 io 原语（请求）

```json
{"method":"kernel.attach","params":{"stream":"stdin","mode":"line"}}  → {}
{"method":"kernel.detach","params":{"stream":"stdin"}}                → {}
{"method":"kernel.write","params":{"stream":"stdout","data":"..."}}   → {}
```

细节在第 9 节。`stream` 只认 `"stdin"` 和 `"stdout"`，其他值是 -32602。

### 4.6 kernel.shutdown（请求）

请求整机下线。内核会**先回 `{}`** 再开始统一关机流程（reason 记为 `ui_quit`）。

### 4.7 插件发给内核的通知

| 方法 | params | 说明 |
|---|---|---|
| `$/stream/chunk` | `{stream_id, seq, data, done}` | 你是提供方，推一块 |
| `$/stream/error` | `{stream_id, code, message, data}` | 你是提供方，流失败了 |
| `$/cancel` | `{stream_id}` 或 `{request_id}` | 放弃你自己发出的调用 |

`$/cancel` 两个字段任选其一: `stream_id` 取消你当调用方的流（内核会连带取消提供方），`request_id` 取消你用 `kernel.invoke` 发出的那次调用（用你当初写的那个字符串 id）。取消是**协作式**的: 内核会停止转发、给提供方发 `$/cancel`，但它无法强制对方停工。
---

## 5. 生命周期与退出

```
内核                                          插件进程
 │  spawn(command, args, cwd, env)             │
 │ ──────────────────────────────────────────► │  起来，开始读 fd 0
 │                                             │
 │  initialize {protocol,plugin_id,config} ──► │
 │ ◄──────────────── {protocol,provides,requires}
 │                                             │
 │  （校验依赖图 + 算拓扑序；有错就整机启动失败） │
 │                                             │
 │  start {capabilities} ────────────────────► │   按拓扑序，依赖在前
 │ ◄──────────────── {}                        │
 │                                             │
 │  invoke / $/event / $/stream/chunk  ◄─────► │   正常工作期
 │                                             │
 │  shutdown {reason} ───────────────────────► │
 │ ◄──────────────── {}                        │
 │                                             │  自己退出
 │  等 shutdown_grace_ms；没退就强杀 → 退出码 2  │
```

顺序是死的: `initialize` → `start` → 工作 → `shutdown`。

- `start` 之前不会有 `invoke` 打到你身上（路由表建立之前内核不发调用）。
- 你想主动停止，就自己退出 —— 但注意下一段。

### 插件意外退出

在 initialize 之后、内核要求你 shutdown 之前自己退出，等于**崩溃**。内核的反应:

1. 所有还在等你回复的调用立刻拿到 -32011，`error.data` 是 `{plugin, exit_code, signal}`。
2. 你作为提供方的那些流被终止，调用方收到 -32011 的 `$/stream/error`。
3. 你作为调用方的那些流被清掉。
4. 发事件 `kernel.plugin.degraded`，然后 `kernel.plugin.stopped`，`payload.reason` 是 `"crash"`。
5. **你的能力槽保留。** 后续调用那个能力仍然得到 -32011，不会偷偷落到别的插件上 —— 这是有意的: 静默改路由比报错更糟。

内核自己不会因为插件死掉而退出，其他插件也不受影响。

### 内核的退出码

| 码 | 含义 |
|---|---|
| 0 | 干净: 所有插件都在 grace 内自己退了，也没有调用被丢下 |
| 2 | 有插件被强杀，或有调用在 drain 预算（`drain_ms`，默认 5000）内没跑完 |

### 关机的内部顺序（你可以依赖它）

1. **drain**: 等在飞的调用跑完，直到 0 个在飞且安静了 `io_eof_idle_ms`（默认 500）; 或到 `drain_ms` 截止。
2. 给每个插件发 `shutdown{reason}`。
3. 等 `shutdown_grace_ms`（默认 5000，取所有插件的最大值）。
4. 强杀剩下的，然后内核退出。

所以: 收到 `shutdown` 后你有 5 秒左右。要落盘、要关连接，现在做，别拖。

---

## 6. 能力调用与路由

- 能力 id 是**不透明字符串**。内核不知道 `"demo.text"` 是什么意思，只知道它归哪个插件、版本多少。名字由你和写配置的人约定。
- 路由表来自两处交叉校验: 配置文件里的 `[capability]` 槽（能力 id -> 插件 id）和插件 `initialize` 时声明的 `provides`。槽必须被它指向的插件真的提供，否则启动失败。
- 一个能力 id 只有一个提供方。没有优先级、没有故障转移、没有负载均衡 —— 要换提供方就改配置（或热重载）。
- 热重载时**整张表被替换**（不是改单条路由），插件通过 `kernel.capabilities.changed` 拿到新表。这意味着: 永远用能力 id 发起调用，不要在本地缓存"哪个插件"，那样在重载后会指向旧世界。
- 调用方标签由内核写死，调用方自己说的不算: 插件调用时是插件 id，宿主调用时是 `"host"`。你不能冒充别的调用方。
- 并发: 每个插件 `max_inflight`（默认 64），全局 `max_inflight_total`（默认 1024）。超了直接 -32019，**不排队** —— 内核宁可让调用方知道自己把对方压垮了，也不悄悄堆队列。
- 内核不重试。要重试是调用方的决定。
- 只有在提供方已经退出或还没 `start` 时才会立刻失败（-32011 / -32014）。
---

## 7. 流

一次调用要么是一问一答，要么是一条流。流是**同一条管道上的通知序列**，不是新的连接。

### 7.1 你当调用方

```json
{"id":"r-1","method":"kernel.invoke",
 "params":{"capability":"demo.text","method":"chat","params":{},
           "meta":{"request_id":"r-1","stream":true}}}
→ {"id":"r-1","result":{"stream_id":"f-3"}}
```

然后:

```json
{"method":"$/stream/chunk","params":{"stream_id":"f-3","seq":0,"data":{"delta":"c0"},"done":false}}
{"method":"$/stream/chunk","params":{"stream_id":"f-3","seq":1,"data":null,"done":true}}
```

要点:

- `stream_id` 是**内核起的名字**（`f-N`），不是你起的。先拿到它，再等块。
- **`seq` 是内核从 0 重新编号的**，提供方自己的 seq 不外传。你只需要按 seq 递增消费，并靠 `done` 判断结束。
- 结束有且只有一个终止块: 要么 `done:true`（`data` 必为 null），要么一条 `$/stream/error{stream_id, code, message, data}`。
- 你也可以主动放弃: 发 `$/cancel{stream_id: "f-3"}`。

### 7.2 你当提供方

收到 `invoke` 且 `meta.stream` 为 true 时:

1. **先回** `{"stream_id": "<你的名字>"}`。这一步**不能省，也不能后置** —— 内核靠这次响应建立 `(你, 你的 stream_id)` → 调用方 stream 的映射。在它之前发的块会被当作孤儿丢掉（内核记一条 warning "sent a chunk for unknown stream"）。
2. 再发任意多块:

```json
{"method":"$/stream/chunk",
 "params":{"stream_id":"s-1","seq":0,"data":{"delta":"hel"},"done":false}}
```

3. 收尾: `{"stream_id":"s-1","seq":1,"data":null,"done":true}`。**`data` 必须是 null** —— 终止块带 payload 会被记 warning 并丢弃那个 data。
4. 或者以错误结束: `{"stream_id":"s-1","code":-32603,"message":"boom","data":{}}`。

关于你自己的 `seq`: 内核**不使用**它（调用方看到的是内核重编号的 seq）。写递增的整数仍然是对的 —— 它是你排查自己问题的依据。

关于名字: 你的 `stream_id` 在**你自己的管道上**唯一即可，内核负责跨插件改名。你可以同时开多条流。

### 7.3 内核替你做的事

- 改名: 你的 `stream_id` → 调用方的 `f-N`。
- 重编号: 调用方看到的 `seq` 从 0 连续递增。
- 背压: 见下。
- 空闲超时: 见下。
- 取消转发、终止块唯一化、提供方崩溃时把 -32011 送成 `$/stream/error`。

### 7.4 背压（你需要知道，因为它会咬你）

- 调用方的出站队列超过 `queue_high_water_bytes`（默认 2 MiB）时，块不再直投，而是暂存在内核里。
- 降到 `queue_low_water_bytes`（默认 1 MiB）以下才放行。巡检间隔约 100 ms，所以这不是零延迟的。
- 暂存有硬顶: `stream_buffer_chunks`（默认 4096 块）和 `stream_buffer_bytes`（默认 8 MiB）。**超了就终止这条流**，调用方收到 -32019 的 `$/stream/error`。
- 调用方读得慢（队列塞满连暂存都放不下）同样是 -32019 终止。
- 宿主（`"host"`）那条队列受完全一样的水位约束 —— 嵌入方读得慢也一样会被节流。

推论: **你不能假设自己推多少对方就收多少。** 长时间快推要能接受被终止，或者自己实现分页/拉取式接口。

### 7.5 空闲超时

一条流超过 `stream_idle_timeout_ms`（默认 30000，可按**提供方插件**覆盖）没有任何块，内核会终止它: 调用方收到 -32012（"stream went idle"），你会收到 `$/cancel`。

所以长任务要**心跳**: 定期发一块（哪怕是空 data），否则会被当成卡死。

### 7.6 取消

- 你取消自己发出的调用: `$/cancel{request_id: "r-1"}`（用你当初写的字符串 id），或 `$/cancel{stream_id: "f-3"}`。
- 内核取消你: `$/cancel{request_id: <数字>}`（内核给提供方的调用号）。
- **取消是协作式的。** 内核做的只有两件事: 停止转发、把 `$/cancel` 转给提供方。它无法强制提供方停工。收到 `$/cancel` 就尽早收尾并停止发块（对已死的流发的块会被丢弃并记 debug 日志）。
- **取消之后没有终止帧** —— `done:true` 和 `$/stream/error` 都不会来。提出取消的那一方知道自己放弃了，不需要内核再通知一次。
- 宿主也能取消它自己的流，同一条通知、同一套参数（见 14.2）。

### 7.7 id 命名空间

| 前缀 | 谁生成 | 用在哪 |
|---|---|---|
| `f-N` | 内核 | 调用方的 stream_id |
| `sub-N` | 内核 | 订阅 id |
| `host-N` | 内核 | 宿主发起的调用（只在宿主侧可见） |

插件自己起的 stream_id 完全自由，只要求在自己的管道上唯一。不要刻意模仿上面的前缀。
---

## 8. 事件

事件是**广播**: 你发的事件送给所有图案匹配的订阅者（包括宿主），你订阅的事件来自所有人。

### 8.1 收事件

```json
{"method":"$/event",
 "params":{"topic":"kernel.plugin.started","seq":12,"payload":{"plugin":"p","pid":4242}}}
```

### 8.2 图案匹配

图案按 `.` 分段，逐段比较:

- `*` 恰好匹配**一段**。
- `**` 是预留写法，**在 v1 里不匹配任何东西**（别指望它做多段通配）。
- 图案和主题的段数必须相等。

| 图案 | 匹配 | 不匹配 |
|---|---|---|
| `demo.*` | `demo.turn` | `demo.turn.started`（段数不等）、`loops.turn` |
| `demo.turn` | `demo.turn` | `demo.turn.started` |
| `*.*` | `demo.turn` | `demo` |
| `kernel.plugin.*` | `kernel.plugin.started` | `kernel.plugin.a.b` |

### 8.3 seq

`seq` 是**总线级**单调递增计数，不是每个主题各自计数。它能帮你发现"我漏了什么"，但不能用来判断"某个主题连续不连续"。

### 8.4 投递预算与丢弃

每个订阅有一份预算，按"已投递未确认"的量和字节计:

| 项 | 默认 |
|---|---|
| `event_queue_len` | 1024 条 |
| `event_queue_bytes` | 4 MiB |

超预算时:

1. **丢掉最新的一条**（不是最老的）。
2. 给你发一条 `kernel.event.dropped`，`payload` 是 `{subscription_id, dropped_count}`，`seq` 固定为 0。
3. 这条丢弃通知**不占预算**，而且是同一订阅**每秒最多一次** —— 它是你发现自己掉队的唯一方式。
4. 你一旦消费（内核确认投递）就归还预算，所以卡住的订阅者会持续触发丢弃。

单帧大小按 `payload 字节数 + topic 长度 + 64` 估算。

**结论: 事件是"尽力而为"，不是可靠队列。** 掉队会被明确告知，但不会补发。要可靠传输就用能力调用。

### 8.5 内核自己发的事件

| topic | payload | 何时 |
|---|---|---|
| `kernel.plugin.started` | `plugin`, `pid` | 某插件 `start` 成功后 |
| `kernel.plugin.stopped` | `plugin`, `reason`（`shutdown` 或 `crash`） | 进程结束 |
| `kernel.plugin.degraded` | `plugin`, `code`, `signal`, `message` | 插件没被要求就退出了 |
| `kernel.capabilities.changed` | `capabilities`（整张新表） | 热重载换了路由表 |
| `kernel.config.reloaded` | `path` | 配置变更被接受 |
| `kernel.event.dropped` | `subscription_id`, `dropped_count` | 你掉队了 |

这些 topic 用 `kernel.` 前缀，插件不能发布它们。想收就先 `kernel.subscribe`，例如 `{"patterns":["kernel.plugin.*"]}`。

### 8.6 热重载与事件

配置文件的变更被接受时，内核会发 `kernel.capabilities.changed`（带整张新表）和 `kernel.config.reloaded`。重载过程中**内核自己不重启**；新增或改动的插件被拉起来、重新 `initialize` + `start`，被替换掉的插件在 drain 之后收到 `shutdown{reason:"reload"}`。重载失败（新配置坏了）会被整体回滚并继续用旧配置跑，你只会看到一条日志，不会有事件。

---

## 9. 终端 io 原语

先说清楚边界，免得你以为内核在偷偷做别的事。

内核的口号是"只负责加载、卸载、依赖"。但**把宿主自己的 stdin/stdout 在多个插件之间多路复用，本质上是传输层的工作**，所以内核把它算进了自己的职责。这一块确实超出了那句口号，这里如实写明。即便如此，内核依然不知道什么叫 UI —— 它只是把字节按所有权转发给某一个插件。

### 9.1 stdin

- **同一时刻只有一个所有者。** `kernel.attach{stream:"stdin"}` 是抢占式的: 原所有者立刻收到 `$/io/detached{stream:"stdin", reason:"taken_over"}`。抢占在一把锁内完成，不会出现两个插件都以为自己拥有 stdin 的窗口。
- **内核只有在有人 attach 之后才开始读自己的 stdin。** 首次 attach 之前一个字节都不读，所以 `echo hi | ...` 的输入会待在操作系统缓冲区里，不会丢。
- 数据以字符串送达:

```json
{"method":"$/io/data","params":{"stream":"stdin","data":"hello\n","eof":false}}
```

- `data` 是**字符串**，不是 base64。非 UTF-8 的字节会被**有损解码**（变成 U+FFFD）—— 想传二进制就别用这个通道，用能力调用传 base64。
- **分块按读取大小切，不按行切**（`io_line_bytes`，默认 8 KiB）。字段名叫 line，别被误导: 一个 `$/io/data` 不等于一行。要按行处理就自己缓冲。
- 宿主 stdin 结束时会先收到一条 `{"data":"","eof":true}`，然后内核开始统一关机（reason = `kernel_exit`）。这是"上游管道关了"的规范路径。
- 读操作在 detach 时会中断；重新 attach 之后，最多可能丢掉一个已读出但还没投递的块。
- `mode`（默认 `"line"`）在 v1 里**只被记录，不影响任何行为**。别依赖它。

### 9.2 stdout

- `kernel.attach{stream:"stdout"}` 声明所有权。stdout 的 `mode` 在 v1 里没有作用。
- 写:

```json
{"method":"kernel.write","params":{"stream":"stdout","data":"\u001b[2J"}}
→ {}
```

  - 只允许 `stream: "stdout"`（其他值 -32602）。
  - 单条 `data` 超过 `event_payload_bytes`（默认 256 KiB）→ -32020。
  - 内核 stdout 队列积压超过 `io_write_queue_bytes`（默认 4 MiB）→ -32019。别把 stdout 当无底洞。
  - `data` 是字符串; 写的是原始字节（UTF-8 编码后）。终端转义序列直接写即可。
- `kernel.detach{stream:"stdout"}` 释放。
- **detach 明确不是关机信号。** 释放 stdout 不会触发下线。
---

## 10. 错误码表

标准 JSON-RPC 五个原样复用，其余是 eggshellmod 自己的块。

| code | 名字 | 含义 |
|---|---|---|
| -32700 | `parse_error` | 帧体不是合法 JSON |
| -32600 | `invalid_request` | 是 JSON 但不是对象; 或既无 `method` 也无 `id` |
| -32601 | `method_not_found` | 内核不认识这个方法 |
| -32602 | `invalid_params` | 参数缺失/不合法 |
| -32603 | `internal_error` | 内核内部错误（例如响应没交到） |
| -32010 | `unknown_capability` | 没有任何插件提供这个能力 id |
| -32011 | `provider_unavailable` | 提供方已退出/不可用 |
| -32012 | `request_timeout` | 调用超时，或流空闲超时 |
| -32013 | `cancelled` | 已取消 |
| -32014 | `not_started` | 提供方还活着但没 `start` |
| -32015 | `protocol_version_mismatch` | `initialize` 的 `protocol` 不是 1 |
| -32016 | `frame_too_large` | 帧超过 `max_frame_bytes` |
| -32017 | `unknown_stream` | 未知的流 id |
| -32018 | `invalid_config` | 配置/启动问题（例如 spawn 失败） |
| -32019 | `overloaded` | 并发、队列或流缓冲到顶 |
| -32020 | `payload_too_large` | payload 超过上限（事件或 `kernel.write`） |
| -32021 | `unknown_subscription` | 订阅 id 不认识 |

-32602 具体会在这些地方出现: `kernel.invoke` 缺字符串 `meta.request_id`、`initialize` 回复里缺 `provides`/`requires`、版本字符串不是合法 semver、订阅 `patterns` 为空、io `stream` 不是 `stdin`/`stdout`、`kernel.write` 的 `stream` 不是 `stdout`、`kernel.unsubscribe` 的 id 写法不合法、`kernel.publish` 用了 `kernel.` 前缀、宿主 `shutdown` 的 `reason` 不在允许的两个值里。

三条注意:

- **-32016 永远不是一条响应。** 帧太大时管道已经不可信，内核记录日志/事件/`--check` 结果，并杀掉插件。
- `-32013`（cancelled）和 `-32017`（unknown_stream）目前在代码里是**保留值**: 表里有名字，正常路径上不会产生。写成插件时按"可能出现"处理即可，别依赖它出现。
- 内核 `--check` 模式下，`-32011` 有特殊含义: 插件在 `initialize` 之后自己退出了。正常运行时 `-32011` 是"提供方已经没了"。

---

## 11. 限制与超时

所有限制都在配置文件的 `[kernel]` 表里，字段名相同。每个都有默认值，只写你要改的。

| 字段 | 默认 | 作用 |
|---|---|---|
| `max_frame_bytes` | 64 MiB | 单帧体上限，超了 -32016 |
| `initialize_timeout_ms` | 5000 | `initialize` 应答上限 |
| `start_timeout_ms` | 10000 | `start` 应答上限 |
| `shutdown_grace_ms` | 5000 | `shutdown` 之后等你退出的时间 |
| `request_timeout_ms` | 30000 | 一次普通能力调用的超时 |
| `stream_idle_timeout_ms` | 30000 | 流多久没块算卡死 |
| `event_payload_bytes` | 256 KiB | `kernel.publish` payload 上限（也是 `kernel.write` 单条上限） |
| `event_queue_len` | 1024 | 每个订阅的条数预算 |
| `event_queue_bytes` | 4 MiB | 每个订阅的字节预算 |
| `outbound_queue_bytes` | 4 MiB | 发往单个插件的出站队列上限 |
| `io_write_queue_bytes` | 4 MiB | 内核 stdout 队列上限 |
| `queue_high_water_bytes` | 2 MiB | 上层水位，超过就暂存流块 |
| `queue_low_water_bytes` | 1 MiB | 下层水位，降到这里才放行 |
| `stream_buffer_chunks` | 4096 | 单条流的暂存块数上限 |
| `stream_buffer_bytes` | 8 MiB | 单条流的暂存字节上限 |
| `max_inflight` | 64 | 每个插件的在飞调用上限 |
| `max_inflight_total` | 1024 | 全局在飞调用上限 |
| `drain_ms` | 5000 | 关机时等在飞调用的预算 |
| `io_eof_idle_ms` | 500 | 关机时"安静多久算干完了" |
| `max_plugins` | 64 | 插件数量上限 |
| `log_line_bytes` | 8 KiB | 日志行截断 / 插件 stderr 行上限 |
| `io_line_bytes` | 8 KiB | 每次读 stdin 的块大小 |

每个插件可以在 `[plugins.<id>]` 里覆盖这几项:

| 字段 | 覆盖 |
|---|---|
| `initialize_timeout_ms` | `initialize` 超时 |
| `start_timeout_ms` | `start` 超时 |
| `shutdown_grace_ms` | 退出宽限 |
| `request_timeout_ms` | 别人调你这个插件时的默认超时 |
| `stream_idle_timeout_ms` | 你提供的流的空闲超时 |
| `max_inflight` | 别人能同时调你多少个 |
---

## 12. 清单与最小插件

### 12.1 内核怎么把你拉起来

配置片段:

```toml
[plugins.demo]
command = "node"                    # 裸名字 → 交给操作系统在 PATH 上找
args = ["plugins/minimal-plugin.js"]
# cwd 缺省是配置文件所在目录
# env = { API_KEY = "${TOKEN}" }    # ${VAR} 会展开；变量没设是硬错误
# clear_env = true                  # 清空继承来的环境变量
# request_timeout_ms = 1000         # 别人调我时的默认超时

[plugins.demo.config]               # 原样出现在 initialize 的 params.config 里
greeting = "hi"

[capability]
"demo.text" = "demo"                # 能力槽 → 插件 id；必须被该插件的 provides 覆盖
```

- `command` 里**带路径分隔符**时按配置文件所在目录解析；**裸名字**留给 PATH（所以 `node` / `python` / `deno` 直接写）。
- `args` 也会做 `${VAR}` 展开，但不会被当成路径解析。
- `${VAR}` 未定义是**硬错误**（宁可拒绝启动，也不要静默变成空串）; `$$` 表示一个字面 `$`; 展开**不递归**。
- 插件 id 不能是 `host`（保留给宿主）。

### 12.2 一个能跑的最小插件（TypeScript / Node）

```ts
#!/usr/bin/env node
// 一个用 Node 写的 eggshell 插件：会说协议，就够了。
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

// 内核关了 stdin（或要求 shutdown 后），正常退出。
process.stdin.on("end", () => process.exit(0));

function handle(message: any): void {
  const { id, method, params } = message;
  if (method === undefined) return;                 // 响应：本示例不主动调别人
  if (id === undefined || id === null) return;      // 通知：不需要回复

  switch (method) {
    case "initialize":
      send({ jsonrpc: "2.0", id,
             result: { protocol: 1, provides, requires } });
      return;

    case "start":
      // params.capabilities 是整张路由表快照，按需缓存。
      send({ jsonrpc: "2.0", id, result: {} });
      return;

    case "invoke": {
      if (params?.meta?.stream !== true) {
        send({ jsonrpc: "2.0", id, result: { echo: params?.params ?? null } });
        return;
      }
      // 流式：必须先回 stream_id，再发块。
      const streamId = `s-${id}`;
      send({ jsonrpc: "2.0", id, result: { stream_id: streamId } });
      for (let n = 0; n < 3; n += 1) {
        chunk(streamId, n, { delta: `chunk ${n}` }, false);
      }
      chunk(streamId, 3, null, true);                // 终止块：data 必须为 null
      return;
    }

    case "shutdown":
      // 所有 reason 一视同仁：收尾、回包、退出。
      send({ jsonrpc: "2.0", id, result: {} });
      process.exit(0);

    default:
      send({ jsonrpc: "2.0", id,
             error: { code: -32601, message: `no ${method} here` } });
  }
}

// 想说话就主动发，例如：
// send({ jsonrpc: "2.0", method: "kernel.log",
//        params: { level: "info", message: "ready", fields: {} } });
```

跑它:

```toml
[plugins.demo]
command = "node"
args = ["minimal-plugin.mjs"]
```

内核测试用的替身插件（`crates/eggshell-kernel/src/bin/eggshell-fixture.rs`，Rust）就是这个形状的另一种语言的实现，遇到不确定的地方可以对着它读。

### 12.3 自查清单

上线之前对着这几条过一遍:

- stdout 上没有任何非帧字节（包括依赖库的 print、进度条、banner）。
- 每个请求都回了同一个 `id`（数字原样带回）。
- `initialize` 回了 `protocol: 1` 和两个数组。
- 流式回复里 `{stream_id}` 先于第一块。
- 终止块 `done:true` 且 `data: null`。
- 长任务有心跳块，不至于撞上 30 秒空闲超时。
- `shutdown` 的四种 reason 行为一致，且真能退出去。
- 日志走 `kernel.log` 或 stderr，不走 stdout。

---

## 13. 版本与兼容

- 当前 `PROTOCOL_VERSION = 1`。
- 版本通过 `initialize` 的 `params.protocol` / `result.protocol` 协商; 任一侧不匹配就是 -32015，启动失败并明确报告双方版本。
- 任何不兼容的线上改动都会递增它。**新增可选字段不算不兼容**（内核忽略不认识的字段），删除或改变已有字段的语义算。
- `kernel_version` 只是诊断信息，不要拿它做特性判断 —— 要判断就判断 `protocol`。---

## 14. 宿主侧：内核当子进程

前面 13 节是"你写插件"的视角。这一节是另一头: **宿主**（比如用 TypeScript 写的 MaoTa）并不链接内核，而是把内核当子进程拉起来，在它的 fd 0 / fd 1 上讲同一套帧。

内核往上也只是一段"插件式"的程序: 宿主拉起它，它拉起插件。往下那一层（插件协议）完全不受影响。

### 14.1 怎么起

```
eggshell <config.toml>
```

这个可执行文件只在 `--features host` 构建时产出。默认构建不产出任何可执行文件（测试替身除外，见 12.2）。

```
宿主 → 内核 fd 0    宿主发的帧（请求）
宿主 ← 内核 fd 1    内核发的帧（回复 + 通知）
        内核 fd 2    日志，一行一个 JSON 对象
```

分帧与第 1 节一字不差（`Content-Length`、8 KiB 头部上限、UTF-8 JSON 体）。插件看不出任何区别: `initialize` / `start` / `invoke` / `shutdown` 都没变。

### 14.2 宿主能发的方法

就这五个，别的回 -32601:

| method | params | result |
|---|---|---|
| `invoke` | `capability`、`method`、`params`，可选 `meta.stream` | 提供方的业务结果；流式为 `{stream_id}` |
| `capabilities` | 无 | `{能力 id: {plugin, version}}` |
| `subscribe` | `patterns`（字符串数组） | `{subscription_id}` |
| `unsubscribe` | `subscription_id` | `{}` |
| `shutdown` | 可选 `reason` | `{}`，**回完这一帧**才开始下线 |

一问一答:

```json
{"jsonrpc":"2.0","id":1,"method":"invoke",
 "params":{"capability":"demo.text","method":"echo","params":{"hi":1}}}
{"jsonrpc":"2.0","id":1,"result":{"got":{"hi":1}}}
```

流式是同一个方法加 `"meta":{"stream":true}`: 回复是 `{"stream_id":"f-3"}`，块以 `$/stream/chunk` 通知送达，`seq` 照样由内核重编号（第 7 节）。事件就是第 8 节那套，只是订阅方是宿主。

宿主 `meta` 里**只有 `stream` 被读**。`timeout_ms` 是插件反向调用内核时的字段，宿主这边没有透传 —— 宿主想精确超时得自己计时。

关机可以带 `reason`:

```json
{"jsonrpc":"2.0","id":9,"method":"shutdown","params":{"reason":"kernel_exit"}}
```

- 只有两个是宿主能诚实给出的: `ui_quit`（不写 `reason` 时的缺省值）和 `kernel_exit`。写 `reload` / `check` 或别的字符串回 -32602，内核继续活着。
- 插件看到的始终是 3.4 那张表里的四个值之一。

放弃一条流就发一条**通知**，方法和参数与插件那侧完全一样（7.6）:

```json
{"jsonrpc":"2.0","method":"$/cancel","params":{"stream_id":"f-3"}}
```

- **取消是静默的**: 内核停止转发、把 `$/cancel` 转给提供方，但**不回**终止帧 —— 是你自己放弃的，别等 `done`。
- 宿主手上的 id 是 `stream_id`（`f-N`，就在回复里）。`request_id` 那条路要写内核内部给宿主调用编的号 `host-N`（见 7.7），一般用不上。
- 取消一个不存在的 id 是无操作: 重复取消、取消一条早就结束的流，都不报错。所以消费者 `break` 的时候顺手取消，是标准动作。

### 14.3 宿主是纯调用方

- 内核替宿主写 `meta.caller = "host"`，提供方可以据此分辨这次调用来自宿主还是别的插件。
- 宿主不提供能力、没有进程、不能当路由目标 —— 配置文件里没有地方能把它写成提供方（`plugins.host` 会被拒绝，见 3.3）。
- 宿主发起的调用号是 `host-N`（见 7.7）。
- 宿主受和插件一样的在飞上限，超了回 -32019（overloaded，第 10 节）。

### 14.4 终端归谁

内核进程的 fd 0 / fd 1 是宿主协议、fd 2 是日志，所以**宿主模式下内核没有终端可以给插件**:

- 宿主发不了 io 原语（`kernel.attach` / `kernel.detach` / `kernel.write`）: 一律 -32601。
- 插件也 attach 不了: 内核拒绝并回 -32601（"the kernel runs as a host subprocess, so its terminal belongs to the host"）。
- 想让插件读键盘、写 tty，v1 没有透传通道。这是已知缺口，不是配置问题。

宿主自己的 stdin / Ctrl-C 是宿主进程的事，内核碰不到。

### 14.5 退出与收尸

- 宿主发 `shutdown` → 内核回 `{}` → 跑统一下线（第 5 节）→ 退出。
- 宿主**关掉 fd 0**（不再发了，或者宿主进程崩了）→ 内核把 EOF 当关机信号，reason = `kernel_exit`，照样干净退出。宿主不需要特地举手告别。
- 退出码: `0` 干净；`2` 有插件被强杀或有调用没 drain 完；`1` 连不上 —— 配置读不了，或者配置读得了但起不来。
- 退出码 `1` 时 stderr 上必定有一行 JSON 报告（`ok:false` + `errors` / `warnings` / `plugins` / `capabilities`）。它和日志行混在一起，用 `ok` 字段认它。
- **绕过 shutdown 直接杀内核进程会留下插件子进程。** 收尸是内核统一下线的一部分。

### 14.6 最小宿主（TypeScript）

形状就是这样，没有别的魔法 —— 起进程、按 `Content-Length` 分帧、收发。MaoTa 里那份桥就是这几行的展开版。

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
  if (frame.id !== undefined) waiting.get(frame.id)?.(frame); // 回复
  else dispatchNotification(frame);                          // $/event、$/stream/chunk
}
```

几条照抄就对的细节:

- fd 2 用 `inherit` 接到宿主自己的 stderr —— 内核日志本来就该落在那里。
- 一条帧可能跨多个 `data` 事件，也可能一次来好几条: 必须缓冲到完整再解析（上面的循环）。
- 内核在 fd 0 上读帧、在 fd 1 上写帧（和插件那一侧对称），所以别让它继承宿主自己的 fd 0: `stdio: ["pipe", "pipe", "inherit"]`。
- `shutdown` 之后不要复用这个内核: 进程会退出去，宿主应该重新 boot。