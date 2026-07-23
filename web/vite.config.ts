import path from "path";
import fileURLToPath from "url";
import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import tailwindcss from "@tailwindcss/vite";

const __dirname = path.dirname(fileURLToPath.fileURLToPath(import.meta.url));

// https://vite.dev
export default defineConfig({
  plugins: [tailwindcss(), vue()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
});
