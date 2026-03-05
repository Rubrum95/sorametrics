module.exports = {
    apps: [
        {
            name: 'sorametrics-api',
            script: 'index.js',
            instances: 1,
            max_memory_restart: '512M',
            restart_delay: 5000,
            exp_backoff_restart_delay: 1000,
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            env: {
                NODE_ENV: 'production',
                PORT: 3000
            }
        },
        {
            name: 'sorametrics-backfill',
            script: 'backfiller.js',
            instances: 1,
            max_memory_restart: '1G',
            restart_delay: 10000,
            exp_backoff_restart_delay: 2000,
            autorestart: true,
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            env: {
                NODE_ENV: 'production'
            }
        }
    ]
};
