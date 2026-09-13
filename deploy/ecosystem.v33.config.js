// PM2 processes for SoraMetrics v33 on Rubrum-01. Binaries come from
// `cargo build --release`; runtime configuration is read from `<cwd>/.env`
// (deploy/env.rubrum.example). The two local validators are given as
// opposite primaries so an API scan never delays the ingest and each
// process fails over to the other node (then mof2).
const cwd = '/root/sorametrics-v33';
const NODE_A = 'ws://127.0.0.1:9944';
const NODE_B = 'ws://127.0.0.1:9945';
const ARCHIVE = 'wss://mof2.sora.org';

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
            env: { ...common.env, API_BIND: '127.0.0.1:3311', WS_ENDPOINTS: `${NODE_B},${NODE_A},${ARCHIVE}` },
        },
        {
            ...common,
            name: 'sorametrics-v33-ingest-substrate',
            script: `${cwd}/target/release/sorametrics-ingest`,
            args: '--source substrate',
            max_memory_restart: '256M',
            env: { ...common.env, WS_ENDPOINTS: `${NODE_A},${NODE_B},${ARCHIVE}` },
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
