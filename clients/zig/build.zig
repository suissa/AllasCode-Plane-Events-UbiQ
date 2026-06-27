const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create a module for other projects to import
    const linear_module = b.addModule("linear", .{
        .root_source_file = b.path("src/linear.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Unit tests
    const lib_unit_tests = b.addTest(.{
        .name = "linear-tests",
        .root_module = linear_module,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // Integration test binary
    const integration_module = b.createModule(.{
        .root_source_file = b.path("src/test_integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_module.addImport("linear", linear_module);

    const integration_exe = b.addExecutable(.{
        .name = "test_integration",
        .root_module = integration_module,
    });

    const run_integration = b.addRunArtifact(integration_exe);
    const run_step = b.step("run-integration", "Run integration tests against localhost:4224");
    run_step.dependOn(&run_integration.step);
}
