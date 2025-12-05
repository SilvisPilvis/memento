const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // We will also create a module for our other entry point, 'main.zig'.
    const exe_mod = b.createModule(.{
        // `root_source_file` is the Zig "entry point" of the module. If a module
        // only contains e.g. external object files, you can make this `null`.
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // This creates another `std.Build.Step.Compile`, but this one builds an executable
    // rather than a static library.
    const exe = b.addExecutable(.{
        .name = "zig_memento",
        .root_module = exe_mod,
    });

    // Add a module from a local file
    const ram_chunking_module = b.addModule("ram_chunking", .{
        .root_source_file = b.path("src/ram_chunking.zig"),
    });

    exe.root_module.addImport("ram_chunking", ram_chunking_module);

    const xxh = b.addModule("xxhash128", .{
        .root_source_file = b.path("src/xxhash128.zig"),
    });

    exe.root_module.addImport("xxhash128", xxh);

    const progress = b.addModule("progress", .{
        .root_source_file = b.path("src/progress.zig"),
    });

    exe.root_module.addImport("progress", progress);

    // Add zstd as a dependency. the string name must mach the dependency key in build.zig.zon
    const zstd_dependency = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
    });

    // Link the static library to the executable
    exe.linkLibrary(zstd_dependency.artifact("zstd"));

    const zstd_module = b.addModule("zstd", .{
        .root_source_file = b.path("src/zstd.zig"),
    });

    exe.root_module.addImport("zstd", zstd_module);

    // const aws_dep = b.dependency("aws", .{
    //     .target = target,
    //     .optimize = optimize,
    // });
    // exe.addModule("aws", aws_dep.module("aws"));

    // This declares intent for the executable to be installed into the
    // standard location when the user invokes the "install" step (the default
    // step when running `zig build`).
    b.installArtifact(exe);

    // This *creates* a Run step in the build graph, to be executed when another
    // step is evaluated that depends on it. The next line below will establish
    // such a dependency.
    const run_cmd = b.addRunArtifact(exe);

    // By making the run step depend on the install step, it will be run from the
    // installation directory rather than directly from within the cache directory.
    // This is not necessary, however, if the application depends on other installed
    // files, this ensures they will be present and in the expected location.
    run_cmd.step.dependOn(b.getInstallStep());

    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build run`
    // This will evaluate the `run` step rather than the default, which is "install".
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const exe_unit_tests = b.addTest(.{
        .root_module = exe_mod,
    });

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_exe_unit_tests.step);
}
