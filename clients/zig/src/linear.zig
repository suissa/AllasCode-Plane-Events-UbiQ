const std = @import("std");
const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

// Header Constants
pub const LinearEventHeader = "Nats-Event-Type";
pub const LinearEventType = "Linear";
pub const LinearTTLHeader = "Nats-Linear-TTL";
pub const LinearOutboxIDHeader = "Nats-Linear-Outbox-Id";
pub const LinearDLQReasonHeader = "Nats-Linear-DLQ-Reason";
pub const LinearDLQOriginalSubjectHeader = "Nats-Linear-Original-Subject";
pub const LinearPQCAlgorithmHeader = "Nats-Linear-PQC-Alg";
pub const LinearPQCPublicKeyHeader = "Nats-Linear-PQC-Public-Key";
pub const DPoPHeader = "DPoP";
pub const linearPQCAlgorithm = "ML-KEM-768";

// Custom Cross-Platform Spin Mutex for Zig 0.17.0
pub const Mutex = struct {
    state: std.atomic.Mutex = .unlocked,

    pub fn lock(self: *Mutex) void {
        while (!self.state.tryLock()) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn unlock(self: *Mutex) void {
        self.state.unlock();
    }
};

// Platform-independent time and sleep helpers
extern "kernel32" fn Sleep(dwMilliseconds: u32) callconv(.winapi) void;
extern "kernel32" fn GetTickCount64() callconv(.winapi) u64;

const timespec = extern struct {
    tv_sec: isize,
    tv_nsec: isize,
};
extern "c" fn nanosleep(rqtp: *const timespec, rmtp: ?*timespec) c_int;
extern "c" fn clock_gettime(clk_id: c_int, tp: *timespec) c_int;

pub fn sleepMs(ms: u64) void {
    if (builtin.os.tag == .windows) {
        Sleep(@intCast(ms));
    } else {
        const ts = timespec{
            .tv_sec = @intCast(ms / 1000),
            .tv_nsec = @intCast((ms % 1000) * 1000000),
        };
        _ = nanosleep(&ts, null);
    }
}

pub fn milliTimestamp() u64 {
    if (builtin.os.tag == .windows) {
        return GetTickCount64();
    } else {
        var ts: timespec = undefined;
        _ = clock_gettime(1, &ts);
        return @intCast(ts.tv_sec * 1000 + @divTrunc(ts.tv_nsec, 1000000));
    }
}

// OS-specific Socket APIs
const windows = struct {
    pub const WSADATA = extern struct {
        wVersion: u16,
        wHighVersion: u16,
        szDescription: [257]u8,
        szSystemStatus: [129]u8,
        iMaxSockets: u16,
        iMaxUdpDg: u16,
        lpVendorInfo: ?*anyopaque,
    };
    pub const sockaddr_in = extern struct {
        sin_family: u16,
        sin_port: u16,
        sin_addr: u32,
        sin_zero: [8]u8 = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 },
    };
    pub extern "ws2_32" fn WSAStartup(wVersionRequired: u16, lpWSAData: *WSADATA) callconv(.winapi) i32;
    pub extern "ws2_32" fn socket(af: i32, type_or: i32, protocol: i32) callconv(.winapi) usize;
    pub extern "ws2_32" fn connect(s: usize, name: *const sockaddr_in, namelen: i32) callconv(.winapi) i32;
    pub extern "ws2_32" fn send(s: usize, buf: [*]const u8, len: i32, flags: i32) callconv(.winapi) i32;
    pub extern "ws2_32" fn recv(s: usize, buf: [*]u8, len: i32, flags: i32) callconv(.winapi) i32;
    pub extern "ws2_32" fn closesocket(s: usize) callconv(.winapi) i32;
};

const posix_c = struct {
    pub const sockaddr_in = extern struct {
        sin_family: u16,
        sin_port: u16,
        sin_addr: u32,
        sin_zero: [8]u8 = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 },
    };
    pub fn socket(domain: i32, type_or: i32, protocol: i32) i32 {
        if (builtin.os.tag == .linux) {
            return @intCast(std.os.linux.socket(@intCast(domain), @intCast(type_or), @intCast(protocol)));
        } else {
            return c_socket(domain, type_or, protocol);
        }
    }
    pub fn connect(fd: i32, addr: *const sockaddr_in, len: u32) i32 {
        if (builtin.os.tag == .linux) {
            return @intCast(std.os.linux.connect(@intCast(fd), @ptrCast(addr), len));
        } else {
            return c_connect(fd, addr, len);
        }
    }
    pub fn send(fd: i32, buf: [*]const u8, len: usize, flags: i32) isize {
        if (builtin.os.tag == .linux) {
            return @intCast(std.os.linux.sendto(@intCast(fd), buf, len, @intCast(flags), null, 0));
        } else {
            return c_send(fd, buf, len, flags);
        }
    }
    pub fn recv(fd: i32, buf: [*]u8, len: usize, flags: i32) isize {
        if (builtin.os.tag == .linux) {
            return @intCast(std.os.linux.recvfrom(@intCast(fd), buf, len, @intCast(flags), null, null));
        } else {
            return c_recv(fd, buf, len, flags);
        }
    }
    pub fn close(fd: i32) i32 {
        if (builtin.os.tag == .linux) {
            return @intCast(std.os.linux.close(@intCast(fd)));
        } else {
            return c_close(fd);
        }
    }

    extern "c" fn c_socket(domain: i32, type_or: i32, protocol: i32) c_int;
    extern "c" fn c_connect(fd: c_int, addr: *const sockaddr_in, len: u32) c_int;
    extern "c" fn c_send(fd: c_int, buf: [*]const u8, len: usize, flags: i32) isize;
    extern "c" fn c_recv(fd: c_int, buf: [*]u8, len: usize, flags: i32) isize;
    extern "c" fn c_close(fd: c_int) c_int;
};

pub const Socket = struct {
    handle: if (builtin.os.tag == .windows) usize else i32,

    pub fn connect(host: []const u8, port: u16) !Socket {
        const port_be = std.mem.nativeToBig(u16, port);
        var ip_bytes: [4]u8 = .{ 127, 0, 0, 1 };
        
        if (std.mem.eql(u8, host, "127.0.0.1") or std.mem.eql(u8, host, "localhost")) {
            ip_bytes = .{ 127, 0, 0, 1 };
        } else {
            var it = std.mem.splitScalar(u8, host, '.');
            var i: usize = 0;
            while (it.next()) |part| {
                if (i >= 4) break;
                ip_bytes[i] = std.fmt.parseInt(u8, part, 10) catch 127;
                i += 1;
            }
        }
        const ip_u32 = (@as(u32, ip_bytes[0])) | (@as(u32, ip_bytes[1]) << 8) | (@as(u32, ip_bytes[2]) << 16) | (@as(u32, ip_bytes[3]) << 24);

        if (builtin.os.tag == .windows) {
            var wsa: windows.WSADATA = undefined;
            _ = windows.WSAStartup(0x0202, &wsa);

            const s = windows.socket(2, 1, 6);
            const INVALID_SOCKET = ~@as(usize, 0);
            if (s == INVALID_SOCKET) return error.SocketCreationFailed;

            const addr = windows.sockaddr_in{
                .sin_family = 2,
                .sin_port = port_be,
                .sin_addr = ip_u32,
            };

            const rc = windows.connect(s, &addr, @sizeOf(windows.sockaddr_in));
            if (rc < 0) {
                _ = windows.closesocket(s);
                return error.ConnectionFailed;
            }
            return Socket{ .handle = s };
        } else {
            const s = posix_c.socket(2, 1, 0);
            if (s < 0) return error.SocketCreationFailed;

            const addr = posix_c.sockaddr_in{
                .sin_family = 2,
                .sin_port = port_be,
                .sin_addr = ip_u32,
            };

            const rc = posix_c.connect(s, &addr, @sizeOf(posix_c.sockaddr_in));
            if (rc < 0) {
                _ = posix_c.close(s);
                return error.ConnectionFailed;
            }
            return Socket{ .handle = s };
        }
    }

    pub fn close(self: Socket) void {
        if (builtin.os.tag == .windows) {
            _ = windows.closesocket(self.handle);
        } else {
            _ = posix_c.close(self.handle);
        }
    }

    pub fn read(self: Socket, buf: []u8) !usize {
        if (builtin.os.tag == .windows) {
            const rc = windows.recv(self.handle, buf.ptr, @intCast(buf.len), 0);
            if (rc < 0) return error.ReadFailed;
            return @intCast(rc);
        } else {
            const rc = posix_c.recv(self.handle, buf.ptr, buf.len, 0);
            if (rc < 0) return error.ReadFailed;
            return @intCast(rc);
        }
    }

    pub fn writeAll(self: Socket, buf: []const u8) !void {
        var sent: usize = 0;
        while (sent < buf.len) {
            if (builtin.os.tag == .windows) {
                const rc = windows.send(self.handle, buf.ptr + sent, @intCast(buf.len - sent), 0);
                if (rc < 0) return error.WriteFailed;
                sent += @intCast(rc);
            } else {
                const rc = posix_c.send(self.handle, buf.ptr + sent, buf.len - sent, 0);
                if (rc < 0) return error.WriteFailed;
                sent += @intCast(rc);
            }
        }
    }
};

pub const SocketReader = struct {
    socket: Socket,
    buf: [4096]u8 = undefined,
    start: usize = 0,
    end: usize = 0,

    pub fn init(socket: Socket) SocketReader {
        return .{ .socket = socket };
    }

    pub fn readByte(self: *SocketReader) !u8 {
        if (self.start >= self.end) {
            const n = try self.socket.read(&self.buf);
            if (n == 0) return error.EndOfStream;
            self.start = 0;
            self.end = n;
        }
        const b = self.buf[self.start];
        self.start += 1;
        return b;
    }

    pub fn readUntilDelimiterOrEof(self: *SocketReader, out_buf: []u8, delimiter: u8) !?[]const u8 {
        var index: usize = 0;
        while (index < out_buf.len) {
            const b = self.readByte() catch |err| {
                if (err == error.EndOfStream) {
                    if (index == 0) return null;
                    return out_buf[0..index];
                }
                return err;
            };
            if (b == delimiter) {
                return out_buf[0..index];
            }
            out_buf[index] = b;
            index += 1;
        }
        return error.BufferFull;
    }

    pub fn readAll(self: *SocketReader, out_buf: []u8) !void {
        var index: usize = 0;
        while (index < out_buf.len) {
            out_buf[index] = try self.readByte();
            index += 1;
        }
    }
};

pub const Header = struct {
    key: []const u8,
    value: []const u8,
};

pub const SecurityOptions = struct {
    dpop_token: ?[]const u8 = null,
    pqc_public_key: ?[]const u8 = null,
};

pub const OutboxOptions = struct {
    max_attempts: usize = 3,
    dlq_subject: ?[]const u8 = null,
    security: ?SecurityOptions = null,
};

pub const OutboxEntry = struct {
    id: []const u8,
    subject: []const u8,
    payload: []const u8,
    ttl_ms: ?u64 = null,
    attempts: usize = 0,
};

pub const LinearMessage = struct {
    allocator: Allocator,
    subject: []const u8,
    reply: ?[]const u8,
    headers: []Header,
    payload: ?[]u8,
    is_linear: bool,
    mutex: Mutex,
    is_destroyed: bool,
    timer: ?Thread,
    stop_timer: bool,

    pub fn init(allocator: Allocator, subject: []const u8, reply: ?[]const u8, headers: []const Header, payload: []const u8, is_linear: bool, ttl_ms: ?u64) !*LinearMessage {
        const self = try allocator.create(LinearMessage);
        errdefer allocator.destroy(self);

        const dup_subject = try allocator.dupe(u8, subject);
        errdefer allocator.free(dup_subject);

        const dup_reply = if (reply) |r| try allocator.dupe(u8, r) else null;
        errdefer if (dup_reply) |r| allocator.free(r);

        var dup_headers = try allocator.alloc(Header, headers.len);
        errdefer {
            for (dup_headers) |h| {
                if (h.key.len > 0) allocator.free(h.key);
                if (h.value.len > 0) allocator.free(h.value);
            }
            allocator.free(dup_headers);
        }

        // Initialize elements to safely clean up in error
        for (dup_headers) |*h| {
            h.key = "";
            h.value = "";
        }

        for (headers, 0..) |h, i| {
            dup_headers[i] = Header{
                .key = try allocator.dupe(u8, h.key),
                .value = try allocator.dupe(u8, h.value),
            };
        }

        const dup_payload = try allocator.dupe(u8, payload);
        errdefer allocator.free(dup_payload);

        self.* = LinearMessage{
            .allocator = allocator,
            .subject = dup_subject,
            .reply = dup_reply,
            .headers = dup_headers,
            .payload = dup_payload,
            .is_linear = is_linear,
            .mutex = .{},
            .is_destroyed = false,
            .timer = null,
            .stop_timer = false,
        };

        if (is_linear and ttl_ms != null and ttl_ms.? > 0) {
            const TimerContext = struct {
                msg: *LinearMessage,
                delay_ms: u64,
                pub fn run(ctx: @This()) void {
                    const start = milliTimestamp();
                    while (true) {
                        ctx.msg.mutex.lock();
                        const done = ctx.msg.is_destroyed or ctx.msg.stop_timer;
                        ctx.msg.mutex.unlock();
                        if (done) break;

                        const now = milliTimestamp();
                        if (@as(u64, @intCast(now - start)) >= ctx.delay_ms) {
                            ctx.msg.destroy();
                            break;
                        }
                        sleepMs(1);
                    }
                }
            };
            self.timer = try Thread.spawn(.{}, TimerContext.run, .{TimerContext{ .msg = self, .delay_ms = ttl_ms.? }});
        }

        return self;
    }

    pub fn deinit(self: *LinearMessage) void {
        self.mutex.lock();
        self.stop_timer = true;
        const timer_thread = self.timer;
        self.timer = null;
        self.mutex.unlock();

        if (timer_thread) |t| {
            t.join();
        }

        // No lock needed here since the timer thread has been joined and we are freeing self.
        self.allocator.free(self.subject);
        if (self.reply) |r| self.allocator.free(r);
        for (self.headers) |h| {
            self.allocator.free(h.key);
            self.allocator.free(h.value);
        }
        self.allocator.free(self.headers);
        if (self.payload) |p| {
            @memset(p, 0);
            self.allocator.free(p);
            self.payload = null;
        }
        self.allocator.destroy(self);
    }

    pub fn access(self: *LinearMessage) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.is_destroyed or self.payload == null) {
            return null;
        }
        const p = self.payload.?;
        if (self.is_linear) {
            // Duplicate payload to return an owned copy that is safe for the caller,
            // while zeroing/destroying our retained reference.
            const result = self.allocator.dupe(u8, p) catch return null;
            @memset(p, 0);
            self.allocator.free(p);
            self.payload = null;
            self.is_destroyed = true;
            return result;
        }
        // For non-linear, return the duplicate so the caller owns it too
        return self.allocator.dupe(u8, p) catch null;
    }

    pub fn destroy(self: *LinearMessage) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.is_destroyed) return;
        if (self.payload) |p| {
            @memset(p, 0);
            self.allocator.free(p);
            self.payload = null;
        }
        self.is_destroyed = true;
    }
};

pub const Outbox = struct {
    allocator: Allocator,
    entries: std.array_list.Managed(OutboxEntry),
    max_attempts: usize,
    dlq_subject: ?[]const u8,
    security: ?SecurityOptions,
    next_id: u64 = 0,

    pub fn init(allocator: Allocator, opts: OutboxOptions) Outbox {
        return Outbox{
            .allocator = allocator,
            .entries = std.array_list.Managed(OutboxEntry).init(allocator),
            .max_attempts = if (opts.max_attempts == 0) 3 else opts.max_attempts,
            .dlq_subject = opts.dlq_subject,
            .security = opts.security,
        };
    }

    pub fn deinit(self: *Outbox) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.id);
            self.allocator.free(entry.subject);
            self.allocator.free(entry.payload);
        }
        self.entries.deinit();
    }

    pub fn enqueue_linear(self: *Outbox, subject: []const u8, payload: []const u8, ttl_ms: ?u64) ![]const u8 {
        self.next_id += 1;
        var buf: [32]u8 = undefined;
        const id_str = try std.fmt.bufPrint(&buf, "{}", .{self.next_id});
        const dup_id = try self.allocator.dupe(u8, id_str);
        errdefer self.allocator.free(dup_id);

        const dup_sub = try self.allocator.dupe(u8, subject);
        errdefer self.allocator.free(dup_sub);

        const dup_pay = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(dup_pay);

        try self.entries.append(OutboxEntry{
            .id = dup_id,
            .subject = dup_sub,
            .payload = dup_pay,
            .ttl_ms = ttl_ms,
            .attempts = 0,
        });
        return dup_id;
    }

    pub fn len(self: Outbox) usize {
        return self.entries.items.len;
    }

    pub fn flush(self: *Outbox, client: anytype) !void {
        var index: usize = 0;
        var first_err: ?anyerror = null;

        while (index < self.entries.items.len) {
            const entry = self.entries.items[index];
            var headers = std.array_list.Managed(Header).init(self.allocator);
            defer headers.deinit();

            try headers.append(Header{ .key = LinearEventHeader, .value = LinearEventType });
            try headers.append(Header{ .key = LinearOutboxIDHeader, .value = entry.id });

            // Store formatted TTL string in a buffer valid for the iteration block
            var ttl_buf: [32]u8 = undefined;
            var ttl_str: []const u8 = "";
            if (entry.ttl_ms) |ttl| {
                ttl_str = try std.fmt.bufPrint(&ttl_buf, "{}", .{ttl});
                try headers.append(Header{ .key = LinearTTLHeader, .value = ttl_str });
            }

            if (self.security) |sec| {
                if (sec.pqc_public_key) |pqc| {
                    try headers.append(Header{ .key = LinearPQCAlgorithmHeader, .value = linearPQCAlgorithm });
                    try headers.append(Header{ .key = LinearPQCPublicKeyHeader, .value = pqc });
                }
                if (sec.dpop_token) |dpop| {
                    try headers.append(Header{ .key = DPoPHeader, .value = dpop });
                }
            }

            if (client.publish_with_headers(entry.subject, entry.payload, headers.items)) |_| {
                self.allocator.free(entry.id);
                self.allocator.free(entry.subject);
                self.allocator.free(entry.payload);
                _ = self.entries.orderedRemove(index);
            } else |err| {
                if (first_err == null) {
                    first_err = err;
                }
                self.entries.items[index].attempts += 1;
                const updated = self.entries.items[index];
                if (updated.attempts >= self.max_attempts and self.dlq_subject != null) {
                    var dlq_headers = std.array_list.Managed(Header).init(self.allocator);
                    defer dlq_headers.deinit();

                    try dlq_headers.append(Header{ .key = LinearOutboxIDHeader, .value = entry.id });
                    try dlq_headers.append(Header{ .key = LinearDLQOriginalSubjectHeader, .value = entry.subject });

                    var err_buf: [256]u8 = undefined;
                    const err_msg = try std.fmt.bufPrint(&err_buf, "{}", .{err});
                    try dlq_headers.append(Header{ .key = LinearDLQReasonHeader, .value = err_msg });

                    if (client.publish_with_headers(self.dlq_subject.?, entry.payload, dlq_headers.items)) |_| {
                        self.allocator.free(entry.id);
                        self.allocator.free(entry.subject);
                        self.allocator.free(entry.payload);
                        _ = self.entries.orderedRemove(index);
                        continue;
                    } else |dlq_err| {
                        if (first_err == null) {
                            first_err = dlq_err;
                        }
                    }
                }
                index += 1;
            }
        }
        if (first_err) |e| {
            return e;
        }
    }
};

pub const QueueLifecycle = struct {
    is_open: bool,
    destroy_subject: []const u8,
    reconnect_every_ms: u64,
    reconnect_for_ms: u64,

    pub fn init(destroy_subject: []const u8, reconnect_for_ms: u64, reconnect_every_ms: u64) QueueLifecycle {
        return QueueLifecycle{
            .is_open = true,
            .destroy_subject = destroy_subject,
            .reconnect_every_ms = if (reconnect_every_ms == 0) 1000 else reconnect_every_ms,
            .reconnect_for_ms = reconnect_for_ms,
        };
    }

    pub fn on_destroy_event(self: *QueueLifecycle, subject: []const u8) void {
        if (self.is_open and std.mem.eql(u8, subject, self.destroy_subject)) {
            self.is_open = false;
        }
    }

    pub fn should_reconnect(self: QueueLifecycle, elapsed_ms: u64) bool {
        return self.is_open and elapsed_ms <= self.reconnect_for_ms;
    }
};

pub const Subscription = struct {
    sid: []const u8,
    subject: []const u8,
    callback: *const fn (*LinearMessage) void,
};

pub const NatsClient = struct {
    allocator: Allocator,
    stream: Socket,
    subscriptions: std.StringHashMap(Subscription),
    mutex: Mutex,
    next_sid: u64 = 0,
    closed: bool = false,
    reader_thread: ?Thread = null,

    pub fn connect(allocator: Allocator, url: []const u8) !*NatsClient {
        var host_buf: [256]u8 = undefined;
        var port: u16 = 4222;
        const host = try parseUrl(url, &host_buf, &port);

        const stream = try Socket.connect(host, port);
        errdefer stream.close();

        const self = try allocator.create(NatsClient);
        errdefer allocator.destroy(self);

        self.* = NatsClient{
            .allocator = allocator,
            .stream = stream,
            .subscriptions = std.StringHashMap(Subscription).init(allocator),
            .mutex = .{},
            .next_sid = 0,
            .closed = false,
            .reader_thread = null,
        };

        // Read server INFO line
        var reader = SocketReader.init(stream);
        var info_buf: [4096]u8 = undefined;
        const info_line = try reader.readUntilDelimiterOrEof(&info_buf, '\n');
        _ = info_line;

        // Send CONNECT
        try stream.writeAll("CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":true}\r\n");

        self.reader_thread = try Thread.spawn(.{}, readLoop, .{self});

        return self;
    }

    pub fn close(self: *NatsClient) void {
        self.mutex.lock();
        if (self.closed) {
            self.mutex.unlock();
            return;
        }
        self.closed = true;
        self.mutex.unlock();

        self.stream.close();

        if (self.reader_thread) |t| {
            t.join();
            self.reader_thread = null;
        }

        self.mutex.lock();
        var it = self.subscriptions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*); // This key is dup_sid
            self.allocator.free(entry.value_ptr.subject);
        }
        self.subscriptions.deinit();
        self.mutex.unlock();

        self.allocator.destroy(self);
    }

    pub fn publish(self: *NatsClient, subject: []const u8, payload: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.ConnectionClosed;

        var buf: [512]u8 = undefined;
        const cmd = try std.fmt.bufPrint(&buf, "PUB {s} {}\r\n", .{ subject, payload.len });
        try self.stream.writeAll(cmd);
        try self.stream.writeAll(payload);
        try self.stream.writeAll("\r\n");
    }

    pub fn publish_with_headers(self: *NatsClient, subject: []const u8, payload: []const u8, headers: []const Header) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.ConnectionClosed;

        var header_block = std.array_list.Managed(u8).init(self.allocator);
        defer header_block.deinit();

        try header_block.appendSlice("NATS/1.0\r\n");
        for (headers) |h| {
            try header_block.appendSlice(h.key);
            try header_block.appendSlice(": ");
            try header_block.appendSlice(h.value);
            try header_block.appendSlice("\r\n");
        }
        try header_block.appendSlice("\r\n");

        const header_len = header_block.items.len;
        const total_len = header_len + payload.len;

        var buf: [512]u8 = undefined;
        const cmd = try std.fmt.bufPrint(&buf, "HPUB {s} {} {}\r\n", .{ subject, header_len, total_len });
        try self.stream.writeAll(cmd);
        try self.stream.writeAll(header_block.items);
        try self.stream.writeAll(payload);
        try self.stream.writeAll("\r\n");
    }

    pub fn subscribe(self: *NatsClient, subject: []const u8, callback: *const fn (*LinearMessage) void) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.ConnectionClosed;

        self.next_sid += 1;
        var sid_buf: [32]u8 = undefined;
        const sid_str = try std.fmt.bufPrint(&sid_buf, "{}", .{self.next_sid});

        const dup_sid = try self.allocator.dupe(u8, sid_str);
        errdefer self.allocator.free(dup_sid);

        const dup_sub = try self.allocator.dupe(u8, subject);
        errdefer self.allocator.free(dup_sub);

        try self.subscriptions.put(dup_sid, Subscription{
            .sid = dup_sid,
            .subject = dup_sub,
            .callback = callback,
        });

        var buf: [512]u8 = undefined;
        const cmd = try std.fmt.bufPrint(&buf, "SUB {s} {s}\r\n", .{ subject, dup_sid });
        try self.stream.writeAll(cmd);

        return try self.allocator.dupe(u8, dup_sid);
    }

    pub fn queue_subscribe(self: *NatsClient, subject: []const u8, queue_group: []const u8, callback: *const fn (*LinearMessage) void) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.ConnectionClosed;

        self.next_sid += 1;
        var sid_buf: [32]u8 = undefined;
        const sid_str = try std.fmt.bufPrint(&sid_buf, "{}", .{self.next_sid});

        const dup_sid = try self.allocator.dupe(u8, sid_str);
        errdefer self.allocator.free(dup_sid);

        const dup_sub = try self.allocator.dupe(u8, subject);
        errdefer self.allocator.free(dup_sub);

        try self.subscriptions.put(dup_sid, Subscription{
            .sid = dup_sid,
            .subject = dup_sub,
            .callback = callback,
        });

        var buf: [512]u8 = undefined;
        const cmd = try std.fmt.bufPrint(&buf, "SUB {s} {s} {s}\r\n", .{ subject, queue_group, dup_sid });
        try self.stream.writeAll(cmd);

        return try self.allocator.dupe(u8, dup_sid);
    }

    fn parseUrl(url: []const u8, host_buf: []u8, port: *u16) ![]const u8 {
        var raw = url;
        if (std.mem.startsWith(u8, raw, "nats://")) {
            raw = raw["nats://".len..];
        }
        var parts = std.mem.splitScalar(u8, raw, ':');
        const host = parts.next() orelse return error.InvalidUrl;
        if (host.len > host_buf.len) return error.HostBufferTooSmall;
        @memcpy(host_buf[0..host.len], host);
        const host_slice = host_buf[0..host.len];

        if (parts.next()) |port_str| {
            port.* = try std.fmt.parseInt(u16, port_str, 10);
        } else {
            port.* = 4222;
        }
        return host_slice;
    }

    fn readLoop(self: *NatsClient) void {
        var reader = SocketReader.init(self.stream);
        var buf: [4096]u8 = undefined;

        while (true) {
            self.mutex.lock();
            const is_closed = self.closed;
            self.mutex.unlock();
            if (is_closed) break;

            const line = reader.readUntilDelimiterOrEof(&buf, '\n') catch |err| {
                self.mutex.lock();
                const was_closed = self.closed;
                self.mutex.unlock();
                if (!was_closed) {
                    std.log.err("read error in NatsClient loop: {}", .{err});
                }
                break;
            } orelse break;

            var line_trimmed = line;
            if (line_trimmed.len > 0 and line_trimmed[line_trimmed.len - 1] == '\r') {
                line_trimmed = line_trimmed[0 .. line_trimmed.len - 1];
            }

            if (line_trimmed.len == 0) continue;

            // Log read line for integration debugging
            std.debug.print("[NATS RECV] {s}\n", .{line_trimmed});

            var parts = std.mem.tokenizeAny(u8, line_trimmed, " ");
            const op = parts.next() orelse continue;

            if (std.mem.eql(u8, op, "PING")) {
                self.mutex.lock();
                self.stream.writeAll("PONG\r\n") catch {};
                self.mutex.unlock();
            } else if (std.mem.eql(u8, op, "MSG")) {
                const subject = parts.next() orelse continue;
                const sid = parts.next() orelse continue;
                const next_part = parts.next() orelse continue;
                var reply: ?[]const u8 = null;
                var size_str = next_part;
                if (parts.next()) |size_part| {
                    reply = next_part;
                    size_str = size_part;
                }
                const size = std.fmt.parseInt(usize, size_str, 10) catch continue;

                const payload = self.allocator.alloc(u8, size) catch continue;
                _ = reader.readAll(payload) catch {
                    self.allocator.free(payload);
                    continue;
                };
                var crlf: [2]u8 = undefined;
                _ = reader.readAll(&crlf) catch {
                    self.allocator.free(payload);
                    continue;
                };

                self.handleMsg(subject, sid, reply, null, payload) catch {};
                self.allocator.free(payload);
            } else if (std.mem.eql(u8, op, "HMSG")) {
                const subject = parts.next() orelse continue;
                const sid = parts.next() orelse continue;
                const p3 = parts.next() orelse continue;
                var reply: ?[]const u8 = null;
                var hdr_size_str = p3;
                var total_size_str = parts.next() orelse continue;
                if (parts.next()) |total_size_part| {
                    reply = p3;
                    hdr_size_str = total_size_str;
                    total_size_str = total_size_part;
                }

                const hdr_size = std.fmt.parseInt(usize, hdr_size_str, 10) catch continue;
                const total_size = std.fmt.parseInt(usize, total_size_str, 10) catch continue;

                const full_buf = self.allocator.alloc(u8, total_size) catch continue;
                _ = reader.readAll(full_buf) catch {
                    self.allocator.free(full_buf);
                    continue;
                };
                var crlf: [2]u8 = undefined;
                _ = reader.readAll(&crlf) catch {
                    self.allocator.free(full_buf);
                    continue;
                };

                const headers_part = full_buf[0..hdr_size];
                const payload_part = full_buf[hdr_size..];

                var headers = std.array_list.Managed(Header).init(self.allocator);
                var header_lines = std.mem.splitSequence(u8, headers_part, "\r\n");
                _ = header_lines.next(); // Skip status line

                while (header_lines.next()) |h_line| {
                    if (h_line.len == 0) continue;
                    const index = std.mem.indexOfScalar(u8, h_line, ':') orelse continue;
                    const k = h_line[0..index];
                    var v = h_line[index + 1 ..];
                    v = std.mem.trim(u8, v, " \t");
                    const dup_k = self.allocator.dupe(u8, k) catch continue;
                    const dup_v = self.allocator.dupe(u8, v) catch continue;
                    headers.append(Header{ .key = dup_k, .value = dup_v }) catch continue;
                }

                self.handleMsg(subject, sid, reply, headers.items, payload_part) catch {};

                for (headers.items) |h| {
                    self.allocator.free(h.key);
                    self.allocator.free(h.value);
                }
                headers.deinit();
                self.allocator.free(full_buf);
            }
        }
    }

    fn handleMsg(self: *NatsClient, subject: []const u8, sid: []const u8, reply: ?[]const u8, headers: ?[]const Header, payload: []const u8) !void {
        self.mutex.lock();
        const sub_opt = self.subscriptions.get(sid);
        self.mutex.unlock();

        if (sub_opt) |sub| {
            var is_linear = false;
            var ttl_ms: ?u64 = null;
            if (headers) |hdrs| {
                for (hdrs) |h| {
                    if (std.mem.eql(u8, h.key, LinearEventHeader) and std.mem.eql(u8, h.value, LinearEventType)) {
                        is_linear = true;
                    }
                    if (std.mem.eql(u8, h.key, LinearTTLHeader)) {
                        ttl_ms = std.fmt.parseInt(u64, h.value, 10) catch null;
                    }
                }
            }

            const msg = try LinearMessage.init(self.allocator, subject, reply, headers orelse &[_]Header{}, payload, is_linear, ttl_ms);
            sub.callback(msg);
        }
    }
};

// ==========================================
// UNIT TESTS
// ==========================================

test "linear_access_is_single_use" {
    const allocator = std.testing.allocator;
    const msg = try LinearMessage.init(allocator, "subject", null, &[_]Header{}, "secret", true, null);
    defer msg.deinit();

    const p1 = msg.access();
    try std.testing.expect(p1 != null);
    try std.testing.expectEqualStrings("secret", p1.?);
    allocator.free(p1.?);

    const p2 = msg.access();
    try std.testing.expect(p2 == null);
}

test "ttl_destroys_unread_linear_payload" {
    const allocator = std.testing.allocator;
    const msg = try LinearMessage.init(allocator, "subject", null, &[_]Header{}, "expires", true, 10);
    defer msg.deinit();

    sleepMs(50);

    const p = msg.access();
    try std.testing.expect(p == null);
}

test "non_linear_payload_is_reusable" {
    const allocator = std.testing.allocator;
    const msg = try LinearMessage.init(allocator, "subject", null, &[_]Header{}, "reusable", false, null);
    defer msg.deinit();

    const p1 = msg.access();
    try std.testing.expect(p1 != null);
    try std.testing.expectEqualStrings("reusable", p1.?);
    allocator.free(p1.?);

    const p2 = msg.access();
    try std.testing.expect(p2 != null);
    try std.testing.expectEqualStrings("reusable", p2.?);
    allocator.free(p2.?);
}

const MockMessage = struct {
    subject: []const u8,
    payload: []const u8,
    headers: []Header,
};

const MockClient = struct {
    allocator: Allocator,
    published: std.array_list.Managed(MockMessage),
    fail_count: usize = 0,

    fn init(allocator: Allocator) MockClient {
        return .{
            .allocator = allocator,
            .published = std.array_list.Managed(MockMessage).init(allocator),
        };
    }

    fn deinit(self: *MockClient) void {
        for (self.published.items) |p| {
            self.allocator.free(p.subject);
            self.allocator.free(p.payload);
            for (p.headers) |h| {
                self.allocator.free(h.key);
                self.allocator.free(h.value);
            }
            self.allocator.free(p.headers);
        }
        self.published.deinit();
    }

    pub fn publish_with_headers(self: *MockClient, subject: []const u8, payload: []const u8, headers: []const Header) !void {
        if (self.fail_count > 0) {
            self.fail_count -= 1;
            return error.PublishFailed;
        }
        var dup_hdrs = try self.allocator.alloc(Header, headers.len);
        for (headers, 0..) |h, i| {
            dup_hdrs[i] = Header{
                .key = try self.allocator.dupe(u8, h.key),
                .value = try self.allocator.dupe(u8, h.value),
            };
        }
        try self.published.append(.{
            .subject = try self.allocator.dupe(u8, subject),
            .payload = try self.allocator.dupe(u8, payload),
            .headers = dup_hdrs,
        });
    }
};

test "outbox_success_removes_entry" {
    const allocator = std.testing.allocator;
    var client = MockClient.init(allocator);
    defer client.deinit();

    var outbox = Outbox.init(allocator, OutboxOptions{});
    defer outbox.deinit();

    _ = try outbox.enqueue_linear("linear.out", "payload", 25);
    try std.testing.expectEqual(@as(usize, 1), outbox.len());

    try outbox.flush(&client);
    try std.testing.expectEqual(@as(usize, 0), outbox.len());
    try std.testing.expectEqual(@as(usize, 1), client.published.items.len);

    const p = client.published.items[0];
    try std.testing.expectEqualStrings("linear.out", p.subject);
    try std.testing.expectEqualStrings("payload", p.payload);

    var has_linear = false;
    var has_outbox_id = false;
    var has_ttl = false;
    for (p.headers) |h| {
        if (std.mem.eql(u8, h.key, LinearEventHeader) and std.mem.eql(u8, h.value, LinearEventType)) has_linear = true;
        if (std.mem.eql(u8, h.key, LinearOutboxIDHeader) and std.mem.eql(u8, h.value, "1")) has_outbox_id = true;
        if (std.mem.eql(u8, h.key, LinearTTLHeader) and std.mem.eql(u8, h.value, "25")) has_ttl = true;
    }
    try std.testing.expect(has_linear);
    try std.testing.expect(has_outbox_id);
    try std.testing.expect(has_ttl);
}

test "outbox_retry_keeps_entry" {
    const allocator = std.testing.allocator;
    var client = MockClient.init(allocator);
    client.fail_count = 1;
    defer client.deinit();

    var outbox = Outbox.init(allocator, OutboxOptions{ .max_attempts = 2 });
    defer outbox.deinit();

    _ = try outbox.enqueue_linear("linear.out", "payload", null);
    
    const res = outbox.flush(&client);
    try std.testing.expectError(error.PublishFailed, res);
    try std.testing.expectEqual(@as(usize, 1), outbox.len());
    try std.testing.expectEqual(@as(usize, 1), outbox.entries.items[0].attempts);
}

test "outbox_dlq_after_limit" {
    const allocator = std.testing.allocator;
    var client = MockClient.init(allocator);
    client.fail_count = 1;
    defer client.deinit();

    var outbox = Outbox.init(allocator, OutboxOptions{
        .max_attempts = 1,
        .dlq_subject = "linear.dlq",
    });
    defer outbox.deinit();

    _ = try outbox.enqueue_linear("linear.out", "payload", null);

    const res = outbox.flush(&client);
    try std.testing.expectError(error.PublishFailed, res);
    try std.testing.expectEqual(@as(usize, 0), outbox.len());
    try std.testing.expectEqual(@as(usize, 1), client.published.items.len);

    const p = client.published.items[0];
    try std.testing.expectEqualStrings("linear.dlq", p.subject);
    try std.testing.expectEqualStrings("payload", p.payload);

    var has_outbox_id = false;
    var has_orig_sub = false;
    var has_reason = false;
    for (p.headers) |h| {
        if (std.mem.eql(u8, h.key, LinearOutboxIDHeader) and std.mem.eql(u8, h.value, "1")) has_outbox_id = true;
        if (std.mem.eql(u8, h.key, LinearDLQOriginalSubjectHeader) and std.mem.eql(u8, h.value, "linear.out")) has_orig_sub = true;
        if (std.mem.eql(u8, h.key, LinearDLQReasonHeader)) has_reason = true;
    }
    try std.testing.expect(has_outbox_id);
    try std.testing.expect(has_orig_sub);
    try std.testing.expect(has_reason);
}

test "security_headers_are_attached" {
    const allocator = std.testing.allocator;
    var client = MockClient.init(allocator);
    defer client.deinit();

    var outbox = Outbox.init(allocator, OutboxOptions{
        .security = SecurityOptions{
            .dpop_token = "proof.jwt",
            .pqc_public_key = "kyber-public-key",
        },
    });
    defer outbox.deinit();

    _ = try outbox.enqueue_linear("linear.secure", "payload", null);
    try outbox.flush(&client);

    try std.testing.expectEqual(@as(usize, 1), client.published.items.len);
    const p = client.published.items[0];

    var has_pqc_alg = false;
    var has_pqc_key = false;
    var has_dpop = false;
    for (p.headers) |h| {
        if (std.mem.eql(u8, h.key, LinearPQCAlgorithmHeader) and std.mem.eql(u8, h.value, linearPQCAlgorithm)) has_pqc_alg = true;
        if (std.mem.eql(u8, h.key, LinearPQCPublicKeyHeader) and std.mem.eql(u8, h.value, "kyber-public-key")) has_pqc_key = true;
        if (std.mem.eql(u8, h.key, DPoPHeader) and std.mem.eql(u8, h.value, "proof.jwt")) has_dpop = true;
    }
    try std.testing.expect(has_pqc_alg);
    try std.testing.expect(has_pqc_key);
    try std.testing.expect(has_dpop);
}
