import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  js.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.check.json",
      },
    },
    rules: {
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        { allowNumber: true },
      ],
    },
  },
  {
    // Test fakes implement async contracts (SSEEventStore, Redis client
    // surface, retry callbacks), so `async` without `await` is
    // intentional there rather than a bug.
    files: ["tests/**/*.ts"],
    rules: {
      "@typescript-eslint/require-await": "off",
    },
  },
  {
    // Plain-JS fixtures outside the TS project: untyped lint only.
    files: ["tests/fixtures/*.mjs"],
    ...tseslint.configs.disableTypeChecked,
  },
);
