module.exports = {
  apps: [
    {
      name: 'alemdaideia-sync-backend',
      cwd: './backend',
      script: 'src/server.js',
      interpreter: 'node',
      watch: false,
      env: {
        NODE_ENV: 'production',
        PORT: 3015,
      },
    },
  ],
}
