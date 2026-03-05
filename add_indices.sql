-- Script para añadir índices a sorametrics databases
-- Ejecutar en el VPS antes de migrar el código
-- Este script es SEGURO - solo crea índices, no modifica datos

-- ============================================
-- PASO 1: Habilitar WAL mode (mejora escrituras concurrentes)
-- ============================================
PRAGMA journal_mode = WAL;

-- ============================================
-- ÍNDICES PARA database_30d.db (Main DB)
-- Ejecutar: sqlite3 /root/sorametrics/database_30d.db < add_indices.sql
-- ============================================

-- Swaps
CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps(timestamp);
CREATE INDEX IF NOT EXISTS idx_swaps_in_symbol ON swaps(in_symbol);
CREATE INDEX IF NOT EXISTS idx_swaps_out_symbol ON swaps(out_symbol);
CREATE INDEX IF NOT EXISTS idx_swaps_wallet ON swaps(wallet);
CREATE INDEX IF NOT EXISTS idx_swaps_block ON swaps(block);
CREATE INDEX IF NOT EXISTS idx_swaps_timestamp_symbol ON swaps(timestamp, in_symbol, out_symbol);

-- Transfers
CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr);
CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr);
CREATE INDEX IF NOT EXISTS idx_transfers_asset ON transfers(asset_id);
CREATE INDEX IF NOT EXISTS idx_transfers_timestamp_from_to ON transfers(timestamp, from_addr, to_addr);

-- Bridges
CREATE INDEX IF NOT EXISTS idx_bridges_timestamp ON bridges(timestamp);
CREATE INDEX IF NOT EXISTS idx_bridges_sender ON bridges(sender);
CREATE INDEX IF NOT EXISTS idx_bridges_recipient ON bridges(recipient);
CREATE INDEX IF NOT EXISTS idx_bridges_timestamp_sender_recipient ON bridges(timestamp, sender, recipient);

-- Fees
CREATE INDEX IF NOT EXISTS idx_fees_timestamp ON fees(timestamp);
CREATE INDEX IF NOT EXISTS idx_fees_type ON fees(type);

-- Liquidity Events
CREATE INDEX IF NOT EXISTS idx_liquidity_timestamp ON liquidity_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_liquidity_type ON liquidity_events(type);
CREATE INDEX IF NOT EXISTS idx_liquidity_pool ON liquidity_events(pool_base, pool_target);
CREATE INDEX IF NOT EXISTS idx_liquidity_wallet ON liquidity_events(wallet);

-- ============================================
-- COMANDOS PARA EJECUTAR EN VPS:
--
-- 1. Índices + WAL en database_30d.db:
--    sqlite3 /root/sorametrics/database_30d.db < add_indices.sql
--
-- 2. Índices + WAL en database.db (history, 1.1GB):
--    sqlite3 /root/sorametrics/database.db < add_indices.sql
--
-- NOTA: En la DB history (1.1GB), crear los índices puede
-- tomar unos minutos. Es seguro ejecutar en producción.
-- ============================================

-- Verificar índices creados (opcional):
-- .indexes
