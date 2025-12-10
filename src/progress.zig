const std = @import("std");
const time = std.time;

pub const ProgressBar = struct {
    allocator: std.mem.Allocator,
    total: usize,
    current: usize = 0,
    width: usize = 20,
    filled_str: []const u8 = "█",
    blank_str: []const u8 = "-",
    schema: []const u8 = " [:bar] :current/:total :percent% :elapsed",
    start_time: i64,
    last_output_len: usize = 0,
    clean: bool = false,

    pub fn init(allocator: std.mem.Allocator, total: usize) !ProgressBar {
        return ProgressBar{
            .allocator = allocator,
            .total = total,
            .start_time = time.timestamp(),
        };
    }

    pub fn tick(self: *ProgressBar, delta: usize) !void {
        self.current += delta;
        try self.render();
    }

    pub fn update(self: *ProgressBar, ratio: f64) !void {
        self.current = @intFromFloat(@max(0, @min(1, ratio)) * @as(f64, @floatFromInt(self.total)));
        try self.render();
    }

    pub fn render(self: *ProgressBar) !void {
        // Clear previous line and print progress
        std.debug.print("\r", .{});
        for (0..self.last_output_len) |_| {
            std.debug.print(" ", .{});
        }
        std.debug.print("\r", .{});

        const now = time.timestamp();
        const elapsed = now - self.start_time;

        const percent = if (self.total > 0)
            @as(f64, @floatFromInt(self.current)) / @as(f64, @floatFromInt(self.total)) * 100.0
        else
            0.0;

        // Parse schema and replace tokens
        var output = std.ArrayList(u8).initCapacity(self.allocator, 0) catch unreachable;
        defer output.deinit(self.allocator);

        var i: usize = 0;
        while (i < self.schema.len) {
            if (self.schema[i] == ':' and i + 1 < self.schema.len) {
                // Find token end
                var j = i + 1;
                // Stop at spaces, colons, brackets, or percent signs
                while (j < self.schema.len and self.schema[j] != ' ' and self.schema[j] != ':' and self.schema[j] != ']' and self.schema[j] != '%') {
                    // while (j < self.schema.len and self.schema[j] != ' ' and self.schema[j] != ':') {
                    j += 1;
                }

                const token = self.schema[i..j];

                if (std.mem.eql(u8, token, ":bar")) {
                    try self.renderBar(&output);
                } else if (std.mem.eql(u8, token, ":current")) {
                    const current_str = try std.fmt.allocPrint(self.allocator, "{d}", .{self.current});
                    defer self.allocator.free(current_str);
                    try output.appendSlice(self.allocator, current_str);
                } else if (std.mem.eql(u8, token, ":total")) {
                    const total_str = try std.fmt.allocPrint(self.allocator, "{d}", .{self.total});
                    defer self.allocator.free(total_str);
                    try output.appendSlice(self.allocator, total_str);
                } else if (std.mem.eql(u8, token, ":percent")) {
                    const percent_str = try std.fmt.allocPrint(self.allocator, "{d:.1}", .{percent});
                    defer self.allocator.free(percent_str);
                    try output.appendSlice(self.allocator, percent_str);
                } else if (std.mem.eql(u8, token, ":elapsed")) {
                    const elapsed_str = try std.fmt.allocPrint(self.allocator, "{d}s", .{@min(elapsed, 999)});
                    defer self.allocator.free(elapsed_str);
                    try output.appendSlice(self.allocator, elapsed_str);
                } else if (std.mem.eql(u8, token, ":eta")) {
                    const eta = if (self.current > 0 and self.current < self.total)
                        @divTrunc(elapsed * @as(i64, @intCast(self.total - self.current)), @as(i64, @intCast(self.current)))
                    else
                        0;
                    // try output.print("{}s", .{eta});
                    const eta_str = try std.fmt.allocPrint(self.allocator, "{d}s", .{eta});
                    defer self.allocator.free(eta_str);
                    try output.appendSlice(self.allocator, eta_str);
                } else if (std.mem.eql(u8, token, ":filled")) {
                    try self.renderFilled(&output);
                } else if (std.mem.eql(u8, token, ":blank")) {
                    try self.renderBlank(&output);
                } else {
                    try output.appendSlice(self.allocator, token);
                }

                i = j;
            } else {
                try output.append(self.allocator, self.schema[i]);
                i += 1;
            }
        }

        std.debug.print("{s}", .{output.items});
        self.last_output_len = output.items.len;

        if (self.completed()) {
            std.debug.print("\n", .{});
            if (self.clean) {
                std.debug.print("\r", .{});
                for (0..self.last_output_len) |_| {
                    std.debug.print(" ", .{});
                }
                std.debug.print("\r", .{});
            }
        }
    }

    fn printSpaces(self: *ProgressBar, stdout: anytype, count: usize) !void {
        _ = self;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            try stdout.print(" ", .{});
        }
    }

    fn renderBar(self: *ProgressBar, output: *std.ArrayList(u8)) !void {
        try self.renderFilled(output);
        try self.renderBlank(output);
    }

    fn renderFilled(self: *ProgressBar, output: *std.ArrayList(u8)) !void {
        const filled = if (self.total > 0)
            @as(usize, @intFromFloat(@as(f64, @floatFromInt(self.current)) / @as(f64, @floatFromInt(self.total)) * @as(f64, @floatFromInt(self.width))))
        else
            0;

        for (0..filled) |_| {
            try output.appendSlice(self.allocator, self.filled_str);
        }
    }

    fn renderBlank(self: *ProgressBar, output: *std.ArrayList(u8)) !void {
        const filled = if (self.total > 0)
            @as(usize, @intFromFloat(@as(f64, @floatFromInt(self.current)) / @as(f64, @floatFromInt(self.total)) * @as(f64, @floatFromInt(self.width))))
        else
            0;

        for (filled..self.width) |_| {
            try output.appendSlice(self.allocator, self.blank_str);
        }
    }

    pub fn completed(self: *ProgressBar) bool {
        return self.current >= self.total;
    }

    pub fn clear(self: *ProgressBar) !void {
        std.debug.print("\r", .{});
        for (0..self.last_output_len) |_| {
            std.debug.print(" ", .{});
        }
        std.debug.print("\r", .{});
    }

    pub fn setFilledChar(self: *ProgressBar, char: []const u8) void {
        self.filled_str = char;
    }

    pub fn setBlankChar(self: *ProgressBar, char: []const u8) void {
        self.blank_str = char;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // Example 1: Basic progress bar
    {
        var bar = try ProgressBar.init(allocator, 50);
        bar.schema = "Progress: [:bar] :current/:total :percent%";
        bar.width = 30;

        var i: usize = 0;
        while (i < 50) : (i += 1) {
            try bar.tick(1);
            std.time.sleep(50 * std.time.ns_per_ms);
        }
    }

    // Example 2: Custom schema with ETA
    {
        var bar = try ProgressBar.init(allocator, 100);
        bar.schema = "Downloading: [:bar] :percent% | :elapsed elapsed | ETA: :eta";
        bar.setFilledChar("█");
        bar.setBlankChar("░");
        bar.width = 40;

        var i: usize = 0;
        while (i < 100) : (i += 1) {
            try bar.tick(1);
            std.time.sleep(30 * std.time.ns_per_ms);
        }
    }

    // Example 3: Multiple progress bars
    {
        var bar1 = try ProgressBar.init(allocator, 30);
        bar1.schema = "Task 1: [:bar] :current/:total";
        bar1.width = 20;

        var bar2 = try ProgressBar.init(allocator, 45);
        bar2.schema = "Task 2: [:bar] :current/:total";
        bar2.width = 20;

        var i: usize = 0;
        while (i < 45) : (i += 1) {
            if (i < 30) {
                try bar1.tick(1);
            }
            try bar2.tick(1);
            std.time.sleep(50 * std.time.ns_per_ms);

            if (i == 29) {
                var buffer: [64]u8 = undefined;
                var stdout = std.fs.File.stdout().writer(buffer[0..]);
                stdout.interface.print("\n", .{}) catch {};
            }
        }
    }
}
