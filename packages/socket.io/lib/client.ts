import { Decoder, Encoder, Packet, PacketType } from "socket.io-parser";
import debugModule from "debug";
import url from "url";
import type { IncomingMessage } from "http";
import type { Server } from "./index";
import type { Namespace } from "./namespace";
import type { EventsMap } from "./typed-events";
import type { Socket } from "./socket";
import type { SocketId } from "socket.io-adapter";
import type { Socket as RawSocket } from "engine.io";

const debug = debugModule("socket.io:client");

function objectFromEntries(
  entries: Iterable<readonly [PropertyKey, string]>,
): Record<string, string> {
  const obj: Record<string, string> = Object.create(null);
  for (const [key, value] of entries) {
    obj[key as string] = value;
  }
  return obj;
}

interface WriteOptions {
  compress?: boolean;
  volatile?: boolean;
  preEncoded?: boolean;
  wsPreEncoded?: string;
}

type CloseReason =
  | "transport error"
  | "transport close"
  | "forced close"
  | "ping timeout"
  | "parse error";

export class Client<
  ListenEvents extends EventsMap,
  EmitEvents extends EventsMap,
  ServerSideEvents extends EventsMap,
  SocketData = any,
> {
  public readonly conn: RawSocket;

  private readonly id: string;
  private readonly server: Server<
    ListenEvents,
    EmitEvents,
    ServerSideEvents,
    SocketData
  >;
  private readonly encoder: Encoder;
  private readonly decoder: Decoder;
  private sockets: Map<
    SocketId,
    Socket<ListenEvents, EmitEvents, ServerSideEvents, SocketData>
  > = new Map();
  private nsps: Map<
    string,
    Socket<ListenEvents, EmitEvents, ServerSideEvents, SocketData>
  > = new Map();
  private connectTimeout?: NodeJS.Timeout;

  /**
   * Client constructor.
   *
   * @param server instance
   * @param conn
   * @package
   */
  constructor(
    server: Server<ListenEvents, EmitEvents, ServerSideEvents, SocketData>,
    conn: RawSocket,
  ) {
    this.server = server;
    this.conn = conn;
    this.encoder = server.encoder;
    this.decoder = new server._parser.Decoder();
    // @ts-expect-error use of private
    this.id = conn.id;
    this.setup();
  }

  /**
   * @return the reference to the request that originated the Engine.IO connection
   *
   * @public
   */
  public get request(): IncomingMessage {
    return this.conn.request;
  }

  /**
   * Sets up event listeners.
   *
   * @private
   */
  private setup() {
    this.onclose = this.onclose.bind(this);
    this.ondata = this.ondata.bind(this);
    this.onerror = this.onerror.bind(this);
    this.ondecoded = this.ondecoded.bind(this);

    // @ts-ignore
    this.decoder.on("decoded", this.ondecoded);
    this.conn.on("data", this.ondata);
    this.conn.on("error", this.onerror);
    this.conn.on("close", this.onclose);

    // EN: An open transport alone must not keep a client with no joined namespace.
    // ZH: 仅打开传输连接不足以长期保留尚未加入任何命名空间的客户端。
    this.connectTimeout = setTimeout(() => {
      if (this.nsps.size === 0) {
        debug("no namespace joined yet, close the client");
        this.close();
      } else {
        debug("the client has already joined a namespace, nothing to do");
      }
    }, this.server._connectTimeout);
  }

  /**
   * Connects a client to a namespace.
   *
   * @param {String} name - the namespace
   * @param {Object} auth - the auth parameters
   * @private
   */
  private connect(name: string, auth: Record<string, unknown> = {}): void {
    if (this.server._nsps.has(name)) {
      debug("connecting to namespace %s", name);
      return this.doConnect(name, auth);
    }

    this.server._checkNamespace(
      name,
      auth,
      (
        dynamicNspName:
          | Namespace<ListenEvents, EmitEvents, ServerSideEvents, SocketData>
          | false,
      ) => {
        if (dynamicNspName) {
          this.doConnect(name, auth);
        } else {
          debug("creation of namespace %s was denied", name);
          this._packet({
            type: PacketType.CONNECT_ERROR,
            nsp: name,
            data: {
              message: "Invalid namespace",
            },
          });
        }
      },
    );
  }

  /**
   * Connects a client to a namespace.
   *
   * @param name - the namespace
   * @param {Object} auth - the auth parameters
   *
   * @private
   */
  private doConnect(name: string, auth: Record<string, unknown>): void {
    const nsp = this.server.of(name);

    nsp._add(this, auth, (socket) => {
      // EN: Publish routing entries after admission, before user connection handlers.
      // ZH: 准入成功后登记路由，并先于用户的连接回调完成。
      this.sockets.set(socket.id, socket);
      this.nsps.set(nsp.name, socket);

      if (this.connectTimeout) {
        clearTimeout(this.connectTimeout);
        this.connectTimeout = undefined;
      }
    });
  }

  /**
   * Disconnects from all namespaces and closes transport.
   *
   * @private
   */
  _disconnect(): void {
    for (const socket of this.sockets.values()) {
      socket.disconnect();
    }
    this.sockets.clear();
    this.close();
  }

  /**
   * Removes a socket. Called by each `Socket`.
   *
   * @private
   */
  _remove(
    socket: Socket<ListenEvents, EmitEvents, ServerSideEvents, SocketData>,
  ): void {
    if (this.sockets.has(socket.id)) {
      const nsp = this.sockets.get(socket.id)!.nsp.name;
      this.sockets.delete(socket.id);
      this.nsps.delete(nsp);
    } else {
      debug("ignoring remove for %s", socket.id);
    }
  }

  /**
   * Closes the underlying connection.
   *
   * @private
   */
  private close(): void {
    if ("open" === this.conn.readyState) {
      debug("forcing transport close");
      this.conn.close();
      this.onclose("forced server close");
    }
  }

  /**
   * Writes a packet to the transport.
   *
   * @param {Object} packet object
   * @param {Object} opts
   * @private
   */
  _packet(packet: Packet | Packet[], opts: WriteOptions = {}): void {
    if (this.conn.readyState !== "open") {
      debug("ignoring packet write %j", packet);
      return;
    }
    // EN: The parser can split binary events into a header and separate attachments.
    // ZH: 编码器可将二进制事件拆成协议头和独立附件。
    const encodedPackets = opts.preEncoded
      ? (packet as Packet[]) // previous versions of the adapter incorrectly used socket.packet() instead of writeToEngine()
      : this.encoder.encode(packet as Packet);
    this.writeToEngine(encodedPackets, opts);
  }

  private writeToEngine(
    encodedPackets: Array<string | Buffer>,
    opts: WriteOptions,
  ): void {
    if (opts.volatile && !this.conn.transport.writable) {
      debug(
        "volatile packet is discarded since the transport is not currently writable",
      );
      return;
    }
    const packets = Array.isArray(encodedPackets)
      ? encodedPackets
      : [encodedPackets];
    for (const encodedPacket of packets) {
      // EN: Engine.IO wraps each encoded piece in a message packet for transport.
      // ZH: Engine.IO 将每个已编码片段封装为 message 包后传输。
      this.conn.write(encodedPacket, opts);
    }
  }

  /**
   * Called with incoming transport data.
   *
   * @private
   */
  private ondata(data: unknown): void {
    // try/catch is needed for protocol violations (GH-1880)
    try {
      // EN: Binary attachments may need several data events before decoded fires.
      // ZH: 二进制附件可能需要多个 data 事件，收齐后才触发 decoded。
      this.decoder.add(data);
    } catch (e) {
      debug("invalid packet format");
      this.onerror(e);
    }
  }

  /**
   * Called when parser fully decodes a packet.
   *
   * @private
   */
  private ondecoded(packet: Packet): void {
    const { namespace, authPayload } = this._parseNamespace(packet);
    const socket = this.nsps.get(namespace);

    // EN: CONNECT opens a namespace; later packets must target an admitted socket.
    // ZH: CONNECT 用于接入命名空间；后续包必须指向已获准接入的 Socket。
    if (!socket && packet.type === PacketType.CONNECT) {
      this.connect(namespace, authPayload);
    } else if (
      socket &&
      packet.type !== PacketType.CONNECT &&
      packet.type !== PacketType.CONNECT_ERROR
    ) {
      process.nextTick(function () {
        socket._onpacket(packet);
      });
    } else {
      // EN: Invalid packet order closes the shared connection and all its namespaces.
      // ZH: 非法包顺序会关闭共享连接，影响该连接上的所有命名空间。
      debug("invalid state (packet type: %s)", packet.type);
      this.close();
    }
  }

  private _parseNamespace(packet: Packet) {
    if (this.conn.protocol !== 3) {
      return {
        namespace: packet.nsp,
        authPayload: packet.data,
      };
    }
    const url = new URL(packet.nsp, "https://socket.io");
    return {
      namespace: url.pathname,
      authPayload: objectFromEntries(url.searchParams.entries()),
    };
  }

  /**
   * Handles an error.
   *
   * @param {Object} err object
   * @private
   */
  private onerror(err): void {
    for (const socket of this.sockets.values()) {
      socket._onerror(err);
    }
    this.conn.close();
  }

  /**
   * Called upon transport close.
   *
   * @param reason
   * @param description
   * @private
   */
  private onclose(
    reason: CloseReason | "forced server close",
    description?: any,
  ): void {
    debug("client close with reason %s", reason);

    // ignore a potential subsequent `close` event
    this.destroy();

    // EN: A transport close fans out to every namespace socket on this Client.
    // ZH: 传输连接关闭时，此 Client 上的每个命名空间 Socket 都会收到关闭通知。
    // `nsps` and `sockets` are cleaned up seamlessly
    for (const socket of this.sockets.values()) {
      socket._onclose(reason, description);
    }
    this.sockets.clear();

    this.decoder.destroy(); // clean up decoder
  }

  /**
   * Cleans up event listeners.
   * @private
   */
  private destroy(): void {
    this.conn.removeListener("data", this.ondata);
    this.conn.removeListener("error", this.onerror);
    this.conn.removeListener("close", this.onclose);
    // @ts-ignore
    this.decoder.removeListener("decoded", this.ondecoded);

    if (this.connectTimeout) {
      clearTimeout(this.connectTimeout);
      this.connectTimeout = undefined;
    }
  }
}
