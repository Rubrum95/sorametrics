// PM2 processes for SoraMetrics v33, side by side with the Node service
// (Fase 5: the API listens on 3311 while nginx still routes production
// traffic to the Node on 3000). Binaries come from `cargo build --release`;
// runtime configuration is read from `<cwd>/.env` (see .env.example).
const cwd = '/root/sorametrics-v33';

const common = {
    cwd,
    instances: 1,
    autorestart: true,
    restart_delay: 5000,
    exp_backoff_restart_delay: 1000,
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    env: { RUST_LOG: 'info' },
};

module.exports = {
    apps: [
        {
            ...common,
            name: 'sorametrics-v33-api',
            script: `${cwd}/target/release/sorametrics-api`,
            max_memory_restart: '256M',
            env: { ...common.env, API_BIND: '127.0.0.1:3311' },
        },
        {
            ...common,
            name: 'sorametrics-v33-ingest-substrate',
            script: `${cwd}/target/release/sorametrics-ingest`,
            args: '--source substrate',
            max_memory_restart: '256M',
            // A clean exit is deliberate (primary endpoint recovered or
            // cursor stalled): PM2 restarts it and the gap is filled.
            stop_exit_codes: [],
        },
        {
            ...common,
            name: 'sorametrics-v33-ingest-iroha',
            script: `${cwd}/target/release/sorametrics-ingest`,
            args: '--source iroha',
            max_memory_restart: '256M',
        },
    ],
};
