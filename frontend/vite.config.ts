import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // Backend (Spring Boot) listens on 8080 by default — see
      // backend/src/main/resources/application.properties.
      '/api': 'http://localhost:8080',
    },
  },
})
