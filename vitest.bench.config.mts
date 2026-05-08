import codspeedPlugin from "@codspeed/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [codspeedPlugin()],
  test: {
    exclude: ["node_modules", "dist", "examples/**"],
    benchmark: {
      include: ["benchmarks/codspeed/**/*.bench.ts"],
    },
  },
});
