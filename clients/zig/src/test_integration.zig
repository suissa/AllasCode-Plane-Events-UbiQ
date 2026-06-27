const std = @import("std");
const linear = @import("linear");
const NatsClient = linear.NatsClient;
const LinearMessage = linear.LinearMessage;
const Mutex = linear.Mutex;
const sleepMs = linear.sleepMs;
const milliTimestamp = linear.milliTimestamp;

var received_msg: ?[]const u8 = null;
var received_mutex = Mutex{};

fn onMessage(msg: *LinearMessage) void {
    std.debug.print("Subscriber callback invoked!\n", .{});
    received_mutex.lock();
    defer received_mutex.unlock();
    if (msg.access()) |p| {
        received_msg = msg.allocator.dupe(u8, p) catch null;
        std.debug.print("Message payload accessed: '{s}'\n", .{p});
        msg.allocator.free(p);
    } else {
        std.debug.print("Message payload was null or already accessed/expired.\n", .{});
    }
    msg.deinit();
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    std.debug.print("Connecting to NATS at nats://localhost:4224...\n", .{});
    const client = try NatsClient.connect(allocator, "nats://localhost:4224");
    defer client.close();
    std.debug.print("Connected successfully!\n", .{});

    const subject = "linear.test.zig";

    std.debug.print("Subscribing to subject '{s}'...\n", .{subject});
    const sid = try client.subscribe(subject, onMessage);
    allocator.free(sid);
    std.debug.print("Subscribed successfully!\n", .{});

    // Sleep a bit to ensure subscription is processed by NATS server
    sleepMs(200);

    const payload = "hello from zig linear client!";
    std.debug.print("Publishing linear event to '{s}'...\n", .{subject});
    
    const headers = [_]linear.Header{
        .{ .key = linear.LinearEventHeader, .value = linear.LinearEventType },
        .{ .key = linear.LinearTTLHeader, .value = "5000" }, // 5s TTL
    };
    try client.publish_with_headers(subject, payload, &headers);
    std.debug.print("Published successfully!\n", .{});

    // Wait up to 5 seconds for receipt
    std.debug.print("Waiting for subscriber receipt...\n", .{});
    const start = milliTimestamp();
    var success = false;
    while (milliTimestamp() - start < 5000) {
        received_mutex.lock();
        if (received_msg) |msg_payload| {
            if (std.mem.eql(u8, msg_payload, payload)) {
                success = true;
            }
            allocator.free(msg_payload);
            received_mutex.unlock();
            break;
        }
        received_mutex.unlock();
        sleepMs(10);
    }

    if (success) {
        std.debug.print("\n>>> Integration test PASSED! <<<\n\n", .{});
    } else {
        std.debug.print("\n>>> Integration test FAILED! <<<\n\n", .{});
        return error.IntegrationTestFailed;
    }
}
