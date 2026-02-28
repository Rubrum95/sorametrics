// --- SOFT ERROR HANDLING (evita bucles de alertas que congelan el navegador) ---
const _seenErrors = new Set();
function _logOnce(key, ...args) {
    if (_seenErrors.has(key)) return;
    _seenErrors.add(key);
    console.error(...args);
}
window.onerror = function (msg, url, lineNo, columnNo, error) {
    _logOnce(`onerror:${msg}:${lineNo}:${columnNo}`, 'Error:', msg, 'Line:', lineNo, 'Col:', columnNo, error || '');
    return false;
};
window.onunhandledrejection = function (event) {
    _logOnce(`unhandled:${String(event.reason)}`, 'Unhandled rejection:', event.reason);
};

// --- APP VERSION CHECK (FUERZA ACTUALIZACION EN iOS) ---
const APP_VERSION = 'v4.0';
(async function checkVersion() {
    try {
        const res = await fetch('/api/version?t=' + Date.now());
        const data = await res.json();
        if (data.version !== APP_VERSION) {
            console.log(`Version mismatch: server=${data.version}, client=${APP_VERSION}. Forcing update...`);
            // Clear all caches
            if ('caches' in window) {
                const cacheNames = await caches.keys();
                await Promise.all(cacheNames.map(name => caches.delete(name)));
            }
            // Unregister service worker
            if ('serviceWorker' in navigator) {
                const registrations = await navigator.serviceWorker.getRegistrations();
                await Promise.all(registrations.map(reg => reg.unregister()));
            }
            // Force reload from server (not cache)
            window.location.reload(true);
        }
    } catch (e) {
        console.log('Version check failed (offline?):', e);
    }
})();

// --- HELPER DE NETWORK (reduce DNS/hosts externos) ---
const LOCAL_PLACEHOLDER = 'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIzMiIgaGVpZ2h0PSIzMiI+CjxjaXJjbGUgY3g9IjE2IiBjeT0iMTYiIHI9IjE2IiBmaWxsPSIjRTVFN0VCIi8+Cjx0ZXh0IHg9IjE2IiB5PSIyMSIgdGV4dC1hbmNob3I9Im1pZGRsZSIgZm9udC1zaXplPSIxMiIgZmlsbD0iIzlDQTNBRiI+PzwvdGV4dD4KPC9zdmc+';

function getProxyUrl(originalUrl) {
    if (!originalUrl) return LOCAL_PLACEHOLDER;
    if (originalUrl.startsWith('data:')) return originalUrl;
    if (originalUrl.startsWith('/')) return originalUrl;
    // Proxy local para unificar conexiones y evitar miles de DNS lookups
    return `/proxy-image?url=${encodeURIComponent(originalUrl)}`;
}

const socket = io();
socket.on('connect', () => {
    console.log('Connected to WebSocket');
    const statusEl = document.getElementById('connectionStatus');
    if (statusEl) {
        statusEl.innerText = '🟢 Conectado';
        statusEl.style.color = '#10B981';
    }
});
let currentAssetId = null;
let currentPage = 1;
let totalPages = 1;
let currentSwapFilter = null; // Añadido para el filtro

// --- LANGUAGE LOGIC ---
const TRANSLATIONS = {
    es: {
        tab_balance: "Balance", tab_swaps: "Swaps", tab_transfers: "Transferencias", tab_tokens: "Tokens", tab_liquidity: "Liquidez", tab_bridges: "Puentes", total_net_worth: "Patrimonio Total", header_my_wallets: "💼 Mis Wallets", btn_add_wallet: "+ Añadir Wallet", header_watch_wallets: "👀 Wallets Vigiladas", live_transfers: "💸 Transferencias en Vivo", time: "Hora", from: "De", amount: "Cantidad", to: "Para", waiting_activity: "Esperando actividad...", previous: "Anterior", next: "Siguiente", page: "Página", liquidity_pools: "🌊 Liquidez (Pools)", all: "Todo", total_pools: "Total Pools:", pair: "Par", reserves: "Reservas", action: "Acción", loading_pools: "Cargando pools...", live_swaps: "⚡ Swaps en Vivo", all_tokens: "Todos los Tokens", search_token: "Buscar token...", input: "Entrada", output: "Salida", account: "Cuenta", refresh: "Actualizar", favorites: "⭐ Favoritos", search_token_name_symbol_id: "Buscar por nombre, símbolo o ID...", asset: "Activo", price: "Precio", loading: "Cargando...", holders: "Holders", rank: "Rank", wallet: "Wallet", balance: "Saldo", add_wallet: "Añadir Wallet", sora_address: "Dirección SORA", name_alias: "Nombre (Alias)", example_savings: "Ej: Ahorros", type: "Tipo", my_wallet_total: "Mi Billetera (Suman al total)", watched_wallet_tracking: "Observada (Seguimiento)", save_wallet: "Guardar Wallet", wallet: "Wallet", assets: "Activos", swaps: "Swaps", transfers: "Transferencias", other: "Otro", no_wallets_saved: "No tienes billeteras guardadas.", not_watching_wallets: "No estás observando ninguna wallet.", no_favorites_yet: "No tienes favoritos aún.", no_data: "No hay datos.", no_swaps_found: "No se encontraron swaps.", no_assets_found: "No se encontraron activos.", no_recent_swaps: "No hay swaps recientes.", no_recent_transfers: "No hay transferencias recientes.", error_loading_data: "Error al cargar datos", error_loading_pools: "Error al cargar pools", error_loading: "Error al cargar", page_x_of_y: "Página {current} de {total}", filter: "Filtro:", all_tokens_star: "🌟 Todos", received: "⬇ RECIBIDO", sent: "⬆ ENVIADO", accounts: "cuentas", chart: "Gráfico", fees_pie_title: "Distribución de Tarifas", fees_line_title: "Tendencia de Actividad",
        // Nuevas claves
        bridge_contract: "Contrato Bridge", bridge_internal_hash: "Este hash es un ID interno del bridge de SORA. No es visible en Etherscan.", view: "Ver", direction: "Dirección", view_on_subscan: "Ver en Subscan", transaction_hash: "Hash de Transacción", ethereum_request_hash: "Hash de Solicitud Ethereum", origin: "Origen", extrinsic_id: "ID de Extrinsic", ethereum_network: "Red Ethereum", no_external_link: "Sin enlace externo disponible",
        network_stats_btn: "Estadísticas de Red", timeframe: "Temporalidad:", current_block: "Bloque Actual", live: "En Vivo", bridges: "Puentes", time_filtered: "Filtrado", whale_leaderboard: "🐳 Ranking de Acumulación", loading_whales: "Cargando Ballenas...", network_info: "📊 Info de Red", active_accounts: "Cuentas Activas", swap_volume: "Volumen Swaps (KUSD)", lp_volume: "Nuevo Vol. LP", transfer_volume: "Volumen Transferencias", stablecoin_monitor: "⚖️ Monitor de Stablecoins", deviation_target: "* Desviación del objetivo $1.00", trending_tokens: "📈 Tokens en Tendencia", network_fees: "💸 Tarifas de Red Pagadas", total_xor: "Total XOR", total_usd: "Total USD", network: "Red", asset: "Activo",
        providers: "Proveedores", activity: "Actividad", rank: "Rango", wallet: "Billetera", liquidity_shares: "Cuota de Liquidez", pool_details: "Detalles del Pool", no_providers_found: "No se encontraron proveedores", no_activity_found: "No se encontró actividad", shares: "Cuota"
    },
    en: {
        tab_balance: "Balance", tab_swaps: "Swaps", tab_transfers: "Transfers", tab_tokens: "Tokens", tab_liquidity: "Liquidity", tab_bridges: "Bridges", total_net_worth: "Total Net Worth", header_my_wallets: "💼 My Wallets", btn_add_wallet: "+ Add Wallet", header_watch_wallets: "👀 Watched Wallets", live_transfers: "💸 Live Transfers", time: "Time", from: "From", amount: "Amount", to: "To", waiting_activity: "Waiting for activity...", previous: "Previous", next: "Next", page: "Page", liquidity_pools: "🌊 Liquidity (Pools)", all: "All", total_pools: "Total Pools:", pair: "Pair", reserves: "Reserves", action: "Action", loading_pools: "Loading pools...", live_swaps: "⚡ Live Swaps", all_tokens: "All Tokens", search_token: "Search token...", input: "Input", output: "Output", account: "Account", refresh: "Refresh", favorites: "⭐ Favorites", search_token_name_symbol_id: "Search by name, symbol or ID...", asset: "Asset", price: "Price", loading: "Loading...", holders: "Holders", rank: "Rank", wallet: "Wallet", balance: "Balance", add_wallet: "Add Wallet", sora_address: "SORA Address", name_alias: "Name (Alias)", example_savings: "Ex: Savings", type: "Type", my_wallet_total: "My Wallet (Adds to total)", watched_wallet_tracking: "Watched (Tracking)", save_wallet: "Save Wallet", wallet: "Wallet", assets: "Assets", swaps: "Swaps", transfers: "Transfers", other: "Other", no_wallets_saved: "You have no wallets saved.", not_watching_wallets: "You are not watching any wallets.", no_favorites_yet: "You have no favorites yet.", no_data: "No data.", no_swaps_found: "No swaps found.", no_assets_found: "No assets found.", no_recent_swaps: "No recent swaps.", no_recent_transfers: "No recent transfers.", error_loading_data: "Error loading data", error_loading_pools: "Error loading pools", error_loading: "Error loading", page_x_of_y: "Page {current} of {total}", filter: "Filter:", all_tokens_star: "🌟 All", received: "⬇ RECEIVED", sent: "⬆ SENT", accounts: "accounts", chart: "Chart", fees_pie_title: "XOR Burned by Type", fees_line_title: "Activity Trend (Fees)",
        bridge_contract: "Bridge Contract", bridge_internal_hash: "This hash is an internal SORA bridge ID. It is not visible on Etherscan.", view: "View", direction: "Direction", view_on_subscan: "View on Subscan", transaction_hash: "Transaction Hash", ethereum_request_hash: "Ethereum Request Hash", origin: "Origin", extrinsic_id: "Extrinsic ID", ethereum_network: "Ethereum Network", no_external_link: "No external link available",
        network_stats_btn: "Network Stats", timeframe: "Timeframe:", current_block: "Current Block", live: "Live", bridges: "Bridges", time_filtered: "Filtered", whale_leaderboard: "🐳 Whale Accumulation Ranking", loading_whales: "Loading Whales...", network_info: "📊 Network Info", active_accounts: "Active Accounts", swap_volume: "Swap Volume (KUSD)", lp_volume: "New LP Volume", transfer_volume: "Transfer Volume", stablecoin_monitor: "⚖️ Stablecoin Monitor", deviation_target: "* Deviation from $1.00 target", trending_tokens: "📈 Trending Tokens", network_fees: "💸 Network Fees Paid", total_xor: "Total XOR", total_usd: "Total USD", network: "Network", asset: "Asset",
        providers: "Providers", activity: "Activity", rank: "Rank", wallet: "Wallet", liquidity_shares: "Liquidity Shares", pool_details: "Pool Details", no_providers_found: "No providers found", no_activity_found: "No activity found", shares: "Shares"
    },
    jp: {
        tab_balance: "残高", tab_swaps: "スワップ", tab_transfers: "転送", tab_tokens: "トークン", tab_liquidity: "流動性", tab_bridges: "ブリッジ", total_net_worth: "総資産", header_my_wallets: "💼 マイウォレット", btn_add_wallet: "+ ウォレット追加", header_watch_wallets: "👀 監視ウォレット", live_transfers: "💸 ライブ転送", time: "時間", from: "送信元", amount: "金額", to: "送信先", waiting_activity: "アクティビティ待機中...", previous: "前へ", next: "次へ", page: "ページ", liquidity_pools: "🌊 流動性 (プール)", all: "すべて", total_pools: "プール総数:", pair: "ペア", reserves: "準備金", action: "アクション", loading_pools: "プールを読み込み中...", live_swaps: "⚡ ライブスワップ", all_tokens: "すべてのトークン", search_token: "トークン検索...", input: "入力", output: "出力", account: "アカウント", refresh: "更新", favorites: "⭐ お気に入り", search_token_name_symbol_id: "名前、シンボル、IDで検索...", asset: "資産", price: "価格", loading: "読み込み中...", holders: "ホルダー", rank: "ランク", wallet: "ウォレット", balance: "残高", add_wallet: "ウォレット追加", sora_address: "SORAアドレス", name_alias: "名前 (エイリアス)", example_savings: "例: 貯金", type: "タイプ", my_wallet_total: "マイウォレット (合計に加算)", watched_wallet_tracking: "監視 (追跡のみ)", save_wallet: "ウォレット保存", wallet: "ウォレット", assets: "資産", swaps: "スワップ", transfers: "転送", other: "その他", no_wallets_saved: "保存されたウォレットはありません。", not_watching_wallets: "監視中のウォレットはありません。", no_favorites_yet: "お気に入りはまだありません。", no_data: "データなし。", no_swaps_found: "スワップが見つかりません。", no_assets_found: "資産が見つかりません。", no_recent_swaps: "最近のスワップはありません。", no_recent_transfers: "最近の転送はありません。", error_loading_data: "データの読み込みエラー", error_loading_pools: "プールの読み込みエラー", error_loading: "読み込みエラー", page_x_of_y: "ページ {current} / {total}", filter: "フィルター:", all_tokens_star: "🌟 すべて", received: "⬇ 受信", sent: "⬆ 送信", accounts: "アカウント", chart: "チャート", fees_pie_title: "種類別のXOR焼却", fees_line_title: "活動傾向 (手数料)",
        bridge_contract: "ブリッジ契約", bridge_internal_hash: "このハッシュはSORAブリッジの内部IDです。Etherscanでは見えません。", view: "表示", direction: "方向", view_on_subscan: "Subscanで見る", transaction_hash: "トランザクションハッシュ", ethereum_request_hash: "Ethereumリクエストハッシュ", origin: "オリジン", extrinsic_id: "Extrinsic ID", ethereum_network: "イーサリアムネットワーク", no_external_link: "外部リンクなし",
        network_stats_btn: "ネットワーク統計", timeframe: "期間:", current_block: "現在のブロック", live: "ライブ", bridges: "ブリッジ", time_filtered: "フィルター済", whale_leaderboard: "🐳 クジラ蓄積ランキング", loading_whales: "クジラを読み込み中...", network_health: "⚡ ネットワークの状態", global_volume: "グローバルボリューム", active_users: "アクティブユーザー", est_tps: "推定TPS", stablecoin_monitor: "⚖️ ステーブルコイン監視", deviation_target: "* $1.00ターゲットからの乖離", trending_tokens: "📈 トレンドトークン", network_fees: "💸 支払われたネットワーク料金", total_xor: "合計 XOR", total_usd: "合計 USD", bridges: "ブリッジ", network: "ネットワーク", asset: "資産",
        providers: "プロバイダー", activity: "アクティビティ", rank: "ランク", wallet: "ウォレット", liquidity_shares: "流動性シェア", pool_details: "プールの詳細", no_providers_found: "プロバイダーが見つかりません", no_activity_found: "アクティビティが見つかりません", shares: "シェア"
    },
    pt: {
        tab_balance: "Saldo", tab_swaps: "Trocas", tab_transfers: "Transferências", tab_tokens: "Tokens", tab_liquidity: "Liquidez", tab_bridges: "Pontes", total_net_worth: "Patrimônio Total", header_my_wallets: "💼 Minhas Carteiras", btn_add_wallet: "+ Adicionar Carteira", header_watch_wallets: "👀 Carteiras Observadas", live_transfers: "💸 Transferências ao Vivo", time: "Hora", from: "De", amount: "Quantia", to: "Para", waiting_activity: "Aguardando atividade...", previous: "Anterior", next: "Próximo", page: "Página", liquidity_pools: "🌊 Liquidez (Pools)", all: "Todos", total_pools: "Total de Pools:", pair: "Par", reserves: "Reservas", action: "Ação", loading_pools: "Carregando pools...", live_swaps: "⚡ Trocas ao Vivo", all_tokens: "Todos os Tokens", search_token: "Buscar token...", input: "Entrada", output: "Saída", account: "Conta", refresh: "Atualizar", favorites: "⭐ Favoritos", search_token_name_symbol_id: "Buscar por nome, símbolo o ID...", asset: "Ativo", price: "Preço", loading: "Carregando...", holders: "Detentores", rank: "Ranking", wallet: "Carteira", balance: "Saldo", add_wallet: "Adicionar Carteira", sora_address: "Endereço SORA", name_alias: "Nome (Apelido)", example_savings: "Ex: Poupança", type: "Tipo", my_wallet_total: "Minha Carteira (Soma ao total)", watched_wallet_tracking: "Observada (Apenas rastreamento)", save_wallet: "Salvar Carteira", wallet: "Carteira", assets: "Ativos", swaps: "Trocas", transfers: "Transferências", other: "Outro", no_wallets_saved: "Nenhuma carteira salva.", not_watching_wallets: "Você não está observando nenhuma carteira.", no_favorites_yet: "Você ainda não tem favoritos.", no_data: "Sem dados.", no_swaps_found: "Nenhuma troca encontrada.", no_assets_found: "Nenhum ativo encontrado.", no_recent_swaps: "Nenhuma troca recente.", no_recent_transfers: "Nenhuma transferência recente.", error_loading_data: "Erro ao carregar dados", error_loading_pools: "Erro ao carregar pools", error_loading: "Erro ao carregar", page_x_of_y: "Página {current} de {total}", filter: "Filtro:", all_tokens_star: "🌟 Todos", received: "⬇ RECEBIDO", sent: "⬆ ENVIADO", accounts: "contas", chart: "Gráfico", fees_pie_title: "XOR Queimado por Tipo", fees_line_title: "Tendência de Atividade",
        bridge_contract: "Contrato Bridge", bridge_internal_hash: "Este hash é um ID interno da bridge SORA. Não é visível no Etherscan.", view: "Ver", direction: "Direção", view_on_subscan: "Ver no Subscan", transaction_hash: "Hash da Transação", ethereum_request_hash: "Hash de Solicitação Ethereum", origin: "Origem", extrinsic_id: "ID do Extrinsic", ethereum_network: "Rede Ethereum", no_external_link: "Sem link externo disponível",
        network_stats_btn: "Estatísticas da Rede", timeframe: "Período:", current_block: "Bloco Atual", live: "Ao Vivo", bridges: "Pontes", time_filtered: "Filtrado", whale_leaderboard: "🐳 Ranking de Acumulação de Baleias", loading_whales: "Carregando Baleias...", network_health: "⚡ Saúde da Rede", global_volume: "Volume Global", active_users: "Usuários Ativos", est_tps: "TPS Est.", stablecoin_monitor: "⚖️ Monitor de Stablecoins", deviation_target: "* Desvio da meta de $1.00", trending_tokens: "📈 Tokens em Tendência", network_fees: "💸 Taxas de Rede Pagas", total_xor: "Total XOR", total_usd: "Total USD", bridges: "Pontes", network: "Rede", asset: "Ativo"
    },
    it: {
        tab_balance: "Saldo", tab_swaps: "Swap", tab_transfers: "Trasferimenti", tab_tokens: "Token", tab_liquidity: "Liquidità", tab_bridges: "Ponti", total_net_worth: "Patrimonio Totale", header_my_wallets: "💼 I Miei Wallet", btn_add_wallet: "+ Aggiungi Wallet", header_watch_wallets: "👀 Wallet Osservati", live_transfers: "💸 Trasferimenti Live", time: "Ora", from: "Da", amount: "Importo", to: "A", waiting_activity: "In attesa di attività...", previous: "Precedente", next: "Successivo", page: "Pagina", liquidity_pools: "🌊 Liquidità (Pools)", all: "Tutto", total_pools: "Pools Totali:", pair: "Coppia", reserves: "Riserve", action: "Azione", loading_pools: "Caricamento pools...", live_swaps: "⚡ Swap Live", all_tokens: "Tutti i Token", search_token: "Cerca token...", input: "Input", output: "Output", account: "Account", refresh: "Aggiorna", favorites: "⭐ Preferiti", search_token_name_symbol_id: "Cerca per nome, simbolo o ID...", asset: "Asset", price: "Prezzo", loading: "Caricamento...", holders: "Detentori", rank: "Rango", wallet: "Wallet", balance: "Saldo", add_wallet: "Aggiungi Wallet", sora_address: "Indirizzo SORA", name_alias: "Nome (Alias)", example_savings: "Es: Risparmi", type: "Tipo", my_wallet_total: "Mio Wallet (Aggiunge al totale)", watched_wallet_tracking: "Osservato (Solo tracciamento)", save_wallet: "Salva Wallet", wallet: "Wallet", assets: "Asset", swaps: "Swap", transfers: "Trasferimenti", other: "Altro", no_wallets_saved: "Non hai wallet salvati.", not_watching_wallets: "Non stai osservando nessun wallet.", no_favorites_yet: "Non hai ancora preferiti.", no_data: "Nessun dato.", no_swaps_found: "Nessuno swap trovato.", no_assets_found: "Nessun asset trovato.", no_recent_swaps: "Nessuno swap recente.", no_recent_transfers: "Nessun trasferimento recente.", error_loading_data: "Errore caricamento dati", error_loading_pools: "Errore caricamento pools", error_loading: "Errore caricamento", page_x_of_y: "Pagina {current} di {total}", filter: "Filtro:", all_tokens_star: "🌟 Tutti", received: "⬇ RICEVUTO", sent: "⬆ INVIATO", accounts: "account", chart: "Grafico", fees_pie_title: "XOR Bruciato per Tipo", fees_line_title: "Tendenza Attività",
        bridge_contract: "Contratto Bridge", bridge_internal_hash: "Questo hash è un ID interno del bridge SORA. Non è visibile su Etherscan.", view: "Vedi", direction: "Direzione", view_on_subscan: "Vedi su Subscan", transaction_hash: "Hash della Transazione", ethereum_request_hash: "Hash Richiesta Ethereum", origin: "Origine", extrinsic_id: "ID Extrinsic", ethereum_network: "Rete Ethereum", no_external_link: "Nessun link esterno disponibile",
        network_stats_btn: "Statistiche Rete", timeframe: "Intervallo:", current_block: "Blocco Attuale", live: "Live", bridges: "Ponti", time_filtered: "Filtrato", whale_leaderboard: "🐳 Classifica Accumulo Balene", loading_whales: "Caricamento Balene...", network_health: "⚡ Salute della Rete", global_volume: "Volume Globale", active_users: "Utenti Attivi", est_tps: "TPS Stimato", stablecoin_monitor: "⚖️ Monitor Stablecoin", deviation_target: "* Deviazione dal target $1.00", trending_tokens: "📈 Token in Tendenza", network_fees: "💸 Commissioni di Rete Pagate", total_xor: "Totale XOR", total_usd: "Totale USD", bridges: "Ponti", network: "Rete", asset: "Asset"
    },
    tr: {
        tab_balance: "Bakiye", tab_swaps: "Swaplar", tab_transfers: "Transferler", tab_tokens: "Tokenlar", tab_liquidity: "Likidite", tab_bridges: "Köprüler", total_net_worth: "Toplam Varlık", header_my_wallets: "💼 Cüzdanlarım", btn_add_wallet: "+ Cüzdan Ekle", header_watch_wallets: "👀 İzlenen Cüzdanlar", live_transfers: "💸 Canlı Transferler", time: "Zaman", from: "Gönderen", amount: "Miktar", to: "Alıcı", waiting_activity: "Aktivite bekleniyor...", previous: "Önceki", next: "Sonraki", page: "Sayfa", liquidity_pools: "🌊 Likidite (Havuzlar)", all: "Tümü", total_pools: "Toplam Havuz:", pair: "Çift", reserves: "Rezervler", action: "İşlem", loading_pools: "Havuzlar yükleniyor...", live_swaps: "⚡ Canlı Swaplar", all_tokens: "Tüm Tokenlar", search_token: "Token ara...", input: "Giriş", output: "Çıkış", account: "Hesap", refresh: "Yenile", favorites: "⭐ Favoriler", search_token_name_symbol_id: "İsim, sembol veya ID ile ara...", asset: "Varlık", price: "Fiyat", loading: "Yükleniyor...", holders: "Sahipler", rank: "Sıra", wallet: "Cüzdan", balance: "Bakiye", add_wallet: "Cüzdan Ekle", sora_address: "SORA Adresi", name_alias: "İsim (Takma Ad)", example_savings: "Örn: Tasarruf", type: "Tip", my_wallet_total: "Cüzdanım (Toplama eklenir)", watched_wallet_tracking: "İzlenen (Sadece takip)", save_wallet: "Cüzdanı Kaydet", wallet: "Cüzdan", assets: "Varlıklar", swaps: "Swaplar", transfers: "Transferler", other: "Diğer", no_wallets_saved: "Kayıtlı cüzdanınız yok.", not_watching_wallets: "Hiçbir cüzdanı izlemiyorsunuz.", no_favorites_yet: "Henüz favoriniz yok.", no_data: "Veri yok.", no_swaps_found: "Swap bulunamadı.", no_assets_found: "Varlık bulunamadı.", no_recent_swaps: "Son swap yok.", no_recent_transfers: "Son transfer yok.", error_loading_data: "Veri yükleme hatası", error_loading_pools: "Havuz yükleme hatası", error_loading: "Yükleme hatası", page_x_of_y: "Sayfa {current} / {total}", filter: "Filtre:", all_tokens_star: "🌟 Tümü", received: "⬇ ALINAN", sent: "⬆ GÖNDERİLEN", accounts: "hesap", chart: "Grafik", fees_pie_title: "Türe Göre Yakılan XOR", fees_line_title: "Aktivite Trendi (Ücretler)",
        bridge_contract: "Köprü Kontratı", bridge_internal_hash: "Bu hash, SORA köprüsünün dahili kimliğidir. Etherscan'da görünmez.", view: "Görüntüle", direction: "Yön", view_on_subscan: "Subscan'da Gör", transaction_hash: "İşlem Hash'i", ethereum_request_hash: "Ethereum İstek Hash'i", origin: "Köken", extrinsic_id: "Extrinsic ID", ethereum_network: "Ethereum Ağı", no_external_link: "Harici bağlantı yok",
        network_stats_btn: "Ağ İstatistikleri", timeframe: "Zaman Aralığı:", current_block: "Mevcut Blok", live: "Canlı", bridges: "Köprüler", time_filtered: "Filtreli", whale_leaderboard: "🐳 Balina Birikim Liderliği", loading_whales: "Balinalar Yükleniyor...", network_health: "⚡ Ağ Sağlığı", global_volume: "Küresel Hacim", active_users: "Aktif Kullanıcılar", est_tps: "Tahmini TPS", stablecoin_monitor: "⚖️ Stabil Coin İzleyici", deviation_target: "* $1.00 hedefinden sapma", trending_tokens: "📈 Trend Olan Tokenlar", network_fees: "💸 Ödenen Ağ Ücretleri", total_xor: "Toplam XOR", total_usd: "Toplam USD", bridges: "Köprüler", network: "Ağ", asset: "Varlık"
    },
    ur: {
        tab_balance: "بیلنس", tab_swaps: "تبادلے", tab_transfers: "ٹرانسفرز", tab_tokens: "ٹوکنز", tab_liquidity: "لیکویڈیٹی", tab_bridges: "پل", total_net_worth: "کل اثاثے", header_my_wallets: "💼 میرے بٹوے", btn_add_wallet: "+ بٹوا شامل کریں", header_watch_wallets: "👀 دیکھے گئے بٹوے", live_transfers: "💸 لائیو ٹرانسفرز", time: "وقت", from: "سے", amount: "رقم", to: "کو", waiting_activity: "سرگرمی کا انتظار ہے...", previous: "پچھلا", next: "اگلا", page: "صفحہ", liquidity_pools: "🌊 لیکویڈیٹی (پول)", all: "سب", total_pools: "کل پول:", pair: "جوڑا", reserves: "ذخائر", action: "عمل", loading_pools: "پول لوڈ ہو رہے ہیں...", live_swaps: "⚡ لائیو تبادلے", all_tokens: "تمام ٹوکنز", search_token: "ٹوکن تلاش کریں...", input: "ان پٹ", output: "آؤٹ پٹ", account: "اکاؤنٹ", refresh: "ریفریش", favorites: "⭐ پسندیدہ", search_token_name_symbol_id: "نام، علامت یا آئی ڈی سے تلاش کریں...", asset: "اثاثہ", price: "قیمت", loading: "لوڈ ہو رہا ہے...", holders: "ہولڈرز", rank: "درجہ", wallet: "بٹوا", balance: "بیلنس", add_wallet: "بٹوا شامل کریں", sora_address: "SORA ایڈریس", name_alias: "نام (عرف)", example_savings: "مثال: بچت", type: "قسم", my_wallet_total: "میرا بٹوا (کل میں شامل کریں)", watched_wallet_tracking: "دیکھا گیا (صرف ٹریکنگ)", save_wallet: "بٹوا محفوظ کریں", wallet: "بٹوا", assets: "اثاثے", swaps: "تبادلے", transfers: "ٹرانسفرز", other: "دیگر", no_wallets_saved: "آپ کے پاس کوئی محفوظ شدہ بٹوے نہیں ہیں۔", not_watching_wallets: "آپ کسی بٹوے کو نہیں دیکھ رہے ہیں۔", no_favorites_yet: "آپ کے پاس ابھی کوئی پسندیدہ نہیں ہے۔", no_data: "کوئی ڈیٹا نہیں۔", no_swaps_found: "کوئی تبادلہ نہیں ملا۔", no_assets_found: "کوئی اثاثہ نہیں ملا۔", no_recent_swaps: "کوئی حالیہ تبادلہ نہیں۔", no_recent_transfers: "کوئی حالیہ ٹرانسفر نہیں۔", error_loading_data: "ڈیٹا لوڈ کرنے میں خرابی", error_loading_pools: "پولز لوڈ کرنے میں خرابی", error_loading: "لوڈ کرنے میں خرابی", page_x_of_y: "صفحہ {current} از {total}", filter: "فلٹر:", all_tokens_star: "🌟 سب", received: "⬇ موصول ہوا", sent: "⬆ بھیجا گیا", accounts: "اکاؤنٹس", chart: "چارٹ", fees_pie_title: "XOR Burned by Type", fees_line_title: "Activity Trend",
        bridge_contract: "برج کنٹریکٹ", bridge_internal_hash: "یہ ہیش SORA برج کی داخلی آئی ڈی ہے۔ یہ Etherscan پر نظر نہیں آتا۔", view: "دیکھیں", direction: "سمت", view_on_subscan: "Subscan پر دیکھیں", transaction_hash: "ٹرانزیکشن ہیش", ethereum_request_hash: "Ethereum درخواست ہیش", origin: "اصل", extrinsic_id: "Extrinsic ID", ethereum_network: "Ethereum نیٹورک", no_external_link: "کوئی بیرونی لنک نہیں",
        network_stats_btn: "نیٹ ورک کے اعدادوشمار", timeframe: "وقت کی حد:", current_block: "موجودہ بلاک", live: "لائیو", bridges: "پل", time_filtered: "فلٹرڈ", whale_leaderboard: "🐳 وہیل جمع کرنے کی درجہ بندی", loading_whales: "وہیل لوڈ ہو رہی ہیں...", network_health: "⚡ نیٹ ورک کی صحت", global_volume: "عالمی حجم", active_users: "فعال صارفین", est_tps: "تخمینہ شدہ TPS", stablecoin_monitor: "⚖️ اسٹیبل کوائن مانیٹر", deviation_target: "* $1.00 کے ہدف سے انحراف", trending_tokens: "📈 رجحان ساز ٹوکنز", network_fees: "💸 ادا کردہ نیٹ ورک فیس", total_xor: "کل XOR", total_usd: "کل USD", bridges: "پل", network: "نیٹ ورک", asset: "اثاثہ"
    },
    he: {
        tab_balance: "יתרה", tab_swaps: "החלפות", tab_transfers: "העברות", tab_tokens: "אסימונים", tab_liquidity: "נזילות", tab_bridges: "גשרים", total_net_worth: "שווי כולל", header_my_wallets: "💼 הארנקים שלי", btn_add_wallet: "+ הוסף ארנק", header_watch_wallets: "👀 ארנקים במעקב", live_transfers: "💸 העברות בשידור חי", time: "זמן", from: "מאת", amount: "כמות", to: "אל", waiting_activity: "ממתין לפעילות...", previous: "הקודם", next: "הבא", page: "עמוד", liquidity_pools: "🌊 נזילות (Pools)", all: "הכל", total_pools: "סך הכל בריכות:", pair: "צמד", reserves: "רזרבות", action: "פעולה", loading_pools: "טוען בריכות...", live_swaps: "⚡ החלפות בשידור חי", all_tokens: "כל האסימונים", search_token: "חפש אסימון...", input: "קלט", output: "פלט", account: "חשבון", refresh: "רענן", favorites: "⭐ מועדפים", search_token_name_symbol_id: "חפש לפי שם, סמל או מזהה...", asset: "נכס", price: "מחיר", loading: "טוען...", holders: "מחזיקים", rank: "דירוג", wallet: "ארנק", balance: "יתרה", add_wallet: "הוסף ארנק", sora_address: "כתובת SORA", name_alias: "שם (כינוי)", example_savings: "לדוגמה: חיסכון", type: "סוג", my_wallet_total: "הארנק שלי (מוסיף לסך הכל)", watched_wallet_tracking: "במעקב (מעקב בלבד)", save_wallet: "שמור ארנק", wallet: "ארנק", assets: "נכסים", swaps: "החלפות", transfers: "העברות", other: "אחר", no_wallets_saved: "אין לך ארנקים שמורים.", not_watching_wallets: "אינך עוקב אחר אף ארנק.", no_favorites_yet: "אין לך מועדפים עדיין.", no_data: "אין נתונים.", no_swaps_found: "לא נמצאו החלפות.", no_assets_found: "לא נמצאו נכסים.", no_recent_swaps: "אין החלפות אחרונות.", no_recent_transfers: "אין העברות אחרונות.", error_loading_data: "שגיאה בטעינת נתונים", error_loading_pools: "שגיאה בטעינת בריכות", error_loading: "שגיאה בטעינה", page_x_of_y: "עמוד {current} מתוך {total}", filter: "סינון:", all_tokens_star: "🌟 הכל", received: "⬇ התקבל", sent: "⬆ נשלח", accounts: "חשבונות", chart: "טבלה", fees_pie_title: "XOR Burned by Type", fees_line_title: "Activity Trend",
        bridge_contract: "חוזה גשר", bridge_internal_hash: "ה-hash הזה הוא מזהה פנימי של גשר SORA. הוא לא נראה ב-Etherscan.", view: "הצג", direction: "כיוון", view_on_subscan: "צפה ב-Subscan", transaction_hash: "האש עסקה", ethereum_request_hash: "האש בקשת Ethereum", origin: "מקור", extrinsic_id: "מזהה Extrinsic", ethereum_network: "רשת Ethereum", no_external_link: "אין קישור חיצוני זמין",
        network_stats_btn: "סטטיסטיקות רשת", timeframe: "מסגרת זמן:", current_block: "בלוק נוכחי", live: "בשידור חי", bridges: "גשרים", time_filtered: "מסונן", whale_leaderboard: "🐳 דירוג צבירת לווייתנים", loading_whales: "טוען לווייתנים...", network_health: "⚡ בריאות הרשת", global_volume: "נפח גלובלי", active_users: "משתמשים פעילים", est_tps: "TPS מוערך", stablecoin_monitor: "⚖️ צג מטבעות יציבים", deviation_target: "* סטייה מיעד $1.00", trending_tokens: "📈 אסימונים במגמה", network_fees: "💸 עמלות רשת ששולמו", total_xor: "סך הכל XOR", total_usd: "סך הכל USD", bridges: "גשרים", network: "רשת", asset: "נכס"
    },
    ru: {
        tab_balance: "Баланс", tab_swaps: "Свопы", tab_transfers: "Переводы", tab_tokens: "Токены", tab_liquidity: "Ликвидность", tab_bridges: "Мосты", total_net_worth: "Общая стоимость", header_my_wallets: "💼 Мои кошельки", btn_add_wallet: "+ Добавить кошелек", header_watch_wallets: "👀 Отслеживаемые", live_transfers: "💸 Переводы Live", time: "Время", from: "От", amount: "Сумма", to: "Кому", waiting_activity: "Ожидание активности...", previous: "Назад", next: "Вперед", page: "Стр.", liquidity_pools: "🌊 Пулы ликвидности", all: "Все", total_pools: "Всего пулов:", pair: "Пара", reserves: "Резервы", action: "Действие", loading_pools: "Загрузка пулов...", live_swaps: "⚡ Свопы Live", all_tokens: "Все токены", search_token: "Поиск токена...", input: "Вход", output: "Выход", account: "Аккаунт", refresh: "Обновить", favorites: "⭐ Избранное", search_token_name_symbol_id: "Поиск по имени, символу или ID...", asset: "Актив", price: "Цена", loading: "Загрузка...", holders: "Холдеры", rank: "Ранг", wallet: "Кошелек", balance: "Баланс", add_wallet: "Добавить кошелек", sora_address: "Адрес SORA", name_alias: "Имя (Псевдоним)", example_savings: "Напр.: Сбережения", type: "Тип", my_wallet_total: "Мой кошелек (Суммировать)", watched_wallet_tracking: "Отслеживаемый", save_wallet: "Сохранить", wallet: "Кошелек", assets: "Активы", swaps: "Свопы", transfers: "Переводы", other: "Другое", no_wallets_saved: "Нет сохраненных кошельков.", not_watching_wallets: "Вы не отслеживаете кошельки.", no_favorites_yet: "Нет избранных.", no_data: "Нет данных.", no_swaps_found: "Свопы не найдены.", no_assets_found: "Активы не найдены.", no_recent_swaps: "Нет недавних свопов.", no_recent_transfers: "Нет недавних переводов.", error_loading_data: "Ошибка загрузки", error_loading_pools: "Ошибка загрузки пулов", error_loading: "Ошибка", page_x_of_y: "Стр. {current} из {total}", filter: "Фильтр:", all_tokens_star: "🌟 Все", received: "⬇ ПОЛУЧЕНО", sent: "⬆ ОТПРАВЛЕНО", accounts: "счетов", chart: "График", fees_pie_title: "XOR сожженный по типу", fees_line_title: "Тенденция активности",
        bridge_contract: "Контракт моста", bridge_internal_hash: "Этот хеш — внутренний ID моста SORA. Он не отображается в Etherscan.", view: "Посмотреть", direction: "Направление", view_on_subscan: "Посмотреть в Subscan", transaction_hash: "Хеш транзакции", ethereum_request_hash: "Хеш запроса Ethereum", origin: "Источник", extrinsic_id: "Идентификатор Extrinsic", ethereum_network: "Сеть Ethereum", no_external_link: "Нет внешней ссылки",
        network_stats_btn: "Статистика сети", timeframe: "Таймфрейм:", current_block: "Текущий блок", live: "Live", bridges: "Мосты", time_filtered: "Фильтр", whale_leaderboard: "🐳 Таблица лидеров (Киты)", loading_whales: "Загрузка...", network_health: "⚡ Здоровье сети", global_volume: "Глоб. Объем", active_users: "Активные польз.", est_tps: "Оценка TPS", stablecoin_monitor: "⚖️ Монитор стейблкоинов", deviation_target: "* Отклонение от $1.00", trending_tokens: "📈 Тренды", network_fees: "💸 Комиссии сети", total_xor: "Всего XOR", total_usd: "Всего USD", bridges: "Мосты", network: "Сеть", asset: "Актив"
    },
    zh: {
        tab_balance: "余额", tab_swaps: "兑换", tab_transfers: "转账", tab_tokens: "代币", tab_liquidity: "流动性", tab_bridges: "跨链桥", total_net_worth: "总资产", header_my_wallets: "💼 我的钱包", btn_add_wallet: "+ 添加钱包", header_watch_wallets: "👀 观察钱包", live_transfers: "💸 实时转账", time: "时间", from: "发送方", amount: "金额", to: "接收方", waiting_activity: "等待活动...", previous: "上一页", next: "下一页", page: "页", liquidity_pools: "🌊 流动性池", all: "全部", total_pools: "总池数:", pair: "交易对", reserves: "储备", action: "操作", loading_pools: "加载池中...", live_swaps: "⚡ 实时兑换", all_tokens: "所有代币", search_token: "搜索代币...", input: "输入", output: "输出", account: "账户", refresh: "刷新", favorites: "⭐ 收藏", search_token_name_symbol_id: "按名称、符号或ID搜索...", asset: "资产", price: "价格", loading: "加载中...", holders: "持有人", rank: "排名", wallet: "钱包", balance: "余额", add_wallet: "添加钱包", sora_address: "SORA地址", name_alias: "名称 (别名)", example_savings: "例如: 储蓄", type: "类型", my_wallet_total: "我的钱包 (计入总额)", watched_wallet_tracking: "观察 (仅追踪)", save_wallet: "保存钱包", wallet: "钱包", assets: "资产", swaps: "兑换", transfers: "转账", other: "其他", no_wallets_saved: "没有保存的钱包。", not_watching_wallets: "没有观察任何钱包。", no_favorites_yet: "暂无收藏。", no_data: "无数据。", no_swaps_found: "未找到兑换。", no_assets_found: "未找到资产。", no_recent_swaps: "无近期兑换。", no_recent_transfers: "无近期转账。", error_loading_data: "加载数据错误", error_loading_pools: "加载池错误", error_loading: "加载错误", page_x_of_y: "第 {current} 页 / 共 {total} 页", filter: "筛选:", all_tokens_star: "🌟 全部", received: "⬇ 收到", sent: "⬆ 发送", accounts: "账户", chart: "图表", fees_pie_title: "按类型销毁 XOR", fees_line_title: "活动趋势",
        bridge_contract: "桥接合约", bridge_internal_hash: "此哈希是 SORA 桥接的内部 ID，在 Etherscan 上不可见。", view: "查看", direction: "方向", view_on_subscan: "在 Subscan 上查看", transaction_hash: "交易哈希", ethereum_request_hash: "Ethereum 请求哈希", origin: "来源", extrinsic_id: "Extrinsic ID", ethereum_network: "以太坊网络", no_external_link: "没有外部链接",
        network_stats_btn: "网络统计", timeframe: "时间范围:", current_block: "当前区块", live: "实时", bridges: "跨链桥", time_filtered: "已筛选", whale_leaderboard: "🐳 鲸鱼积累排行榜", loading_whales: "加载中...", network_health: "⚡ 网络健康", global_volume: "全球交易量", active_users: "活跃用户", est_tps: "预估 TPS", stablecoin_monitor: "⚖️ 稳定币监控", deviation_target: "* 偏离 $1.00 目标", trending_tokens: "📈 热门代币", network_fees: "💸 网络费用支出", total_xor: "总 XOR", total_usd: "总 USD", bridges: "跨链桥", network: "网络", asset: "资产"
    }
};

let currentLang = localStorage.getItem('sora_lang') || 'es';

function applyTranslations() {
    document.querySelectorAll('[data-i18n]').forEach(element => {
        const key = element.getAttribute('data-i18n');
        if (TRANSLATIONS[currentLang] && TRANSLATIONS[currentLang][key]) {
            element.innerText = TRANSLATIONS[currentLang][key];
        }
    });
    document.querySelectorAll('[data-i18n-placeholder]').forEach(element => {
        const key = element.getAttribute('data-i18n-placeholder');
        if (TRANSLATIONS[currentLang] && TRANSLATIONS[currentLang][key]) {
            element.placeholder = TRANSLATIONS[currentLang][key];
        }
    });

    const pageIndicatorElements = document.querySelectorAll('[data-i18n="page_x_of_y"]');
    pageIndicatorElements.forEach(el => {
        const text = el.innerText;
        const match = text.match(/(\d+)\s*\/\s*(\d+)/);
        if (match) {
            el.innerText = TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', match[1]).replace('{total}', match[2]);
        } else if (text.includes('Página') || text.includes('Page')) {
            el.innerText = TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', '1').replace('{total}', '1');
        }
    });

    const swapDropdownBtn = document.getElementById('swapDropdownBtn');
    if (swapDropdownBtn) {
        if (currentSwapFilter) {
            swapDropdownBtn.querySelector('span').innerText = `${TRANSLATIONS[currentLang].filter} ${currentSwapFilter}`;
        } else {
            swapDropdownBtn.querySelector('span').innerText = TRANSLATIONS[currentLang].all_tokens;
        }
    }

    const totalHoldersBadge = document.getElementById('totalHoldersBadge');
    if (totalHoldersBadge) {
        const currentText = totalHoldersBadge.innerText;
        const match = currentText.match(/(\d+)/);
        if (match) {
            totalHoldersBadge.innerText = `${match[1]} ${TRANSLATIONS[currentLang].accounts}`;
        }
    }
}

function changeLanguage(lang) {
    if (!TRANSLATIONS[lang]) return;
    currentLang = lang;
    localStorage.setItem('sora_lang', lang);
    applyTranslations();
    renderTabs(); // Re-render tabs to update button text

    // Also update sidebar if open
    if (typeof renderSidebar === 'function') {
        renderSidebar();
    }
}

const LANG_CODES = { es: 'ES', en: 'EN', jp: 'JP', he: 'HE', ru: 'RU', zh: 'ZH', ur: 'UR', pt: 'PT', it: 'IT', tr: 'TR' };

function toggleLangDropdown() {
    document.getElementById('langDropdownContent').classList.toggle('show');
}

window.onclick = function (event) {
    if (!event.target.closest('#langDropdown')) {
        const openDropdown = document.getElementById('langDropdownContent');
        if (openDropdown && openDropdown.classList.contains('show')) {
            openDropdown.classList.remove('show');
        }
    }
    if (!event.target.closest('#swapTokenDropdown')) {
        const openDropdown = document.getElementById('swapDropdownContent');
        if (openDropdown && openDropdown.classList.contains('show')) {
            openDropdown.classList.remove('show');
        }
    }
}

function changeLanguage(lang) {
    currentLang = lang;
    localStorage.setItem('sora_lang', lang);
    document.documentElement.lang = lang; // Vital para CSS :lang()
    if (lang === 'he') document.body.setAttribute('dir', 'rtl'); else document.body.setAttribute('dir', 'ltr');

    const code = LANG_CODES[lang] || 'ES';
    document.getElementById('currentLangFlag').innerText = code;
    document.getElementById('langDropdownContent').classList.remove('show');
    applyTranslations();

    // Re-render navigation to update button text
    if (typeof renderTabs === 'function') renderTabs();
    if (typeof renderSidebar === 'function') renderSidebar();

    // Re-render charts to update titles if they exist
    if (typeof renderFeeCharts === 'function' && document.getElementById('chart-fees-dist')) {
        // We might need to reload fees to get fresh translation, or just re-render.
        // Calling loadNetworkFees() is safest as it re-fetches and re-renders.
        loadNetworkFees();
    }

    if (document.getElementById('balance')) { if (document.getElementById('balance').classList.contains('active')) loadBalanceTab(); }
    if (document.getElementById('tokens')) { if (document.getElementById('tokens').classList.contains('active')) loadTokens(); }
    if (document.getElementById('liquidity')) { if (document.getElementById('liquidity').classList.contains('active')) { loadPools(); loadGlobalLiquidity(); } }
    if (document.getElementById('transfers')) { if (document.getElementById('transfers').classList.contains('active')) loadGlobalTransfers(); }
    if (document.getElementById('swaps')) { if (document.getElementById('swaps').classList.contains('active')) loadGlobalSwaps(); }
}

document.addEventListener('DOMContentLoaded', () => {
    const code = LANG_CODES[currentLang] || 'ES';
    const el = document.getElementById('currentLangFlag');
    if (el) el.innerText = code;
    applyTranslations();
    initNavigation();
});

// --- SWAP DROPDOWN LOGIC (NUEVO) ---
function toggleSwapDropdown() {
    const dropdown = document.getElementById('swapDropdownContent');
    if (dropdown) {
        dropdown.classList.toggle('show');
        if (dropdown.classList.contains('show')) populateSwapTokenList();
    }
}


function selectSwapToken(symbol) {
    currentSwapFilter = symbol;
    swapPage = 1;
    const btnText = document.querySelector('#swapDropdownBtn span');
    if (btnText) btnText.innerText = `${TRANSLATIONS[currentLang].filter} ${symbol}`;
    document.getElementById('swapDropdownContent').classList.remove('show');
    loadGlobalSwaps();
}

function resetSwapFilter() {
    currentSwapFilter = null;
    swapPage = 1;
    const btnText = document.querySelector('#swapDropdownBtn span');
    if (btnText) btnText.innerText = TRANSLATIONS[currentLang].all_tokens;
    document.getElementById('swapDropdownContent').classList.remove('show');
    loadGlobalSwaps();
}


let swapSearchTimeout = null;
let _swapLastQuery = null;

// En vez de descargar TODOS los tokens (y sus logos) de golpe, hacemos búsqueda en servidor.
// Esto evita miles de DNS lookups e imágenes cargando a la vez.
async function updateSwapTokenList(query) {
    const listDiv = document.getElementById('swapTokenList');
    if (!listDiv) return;

    const q = (query || '').trim();
    // Evita repetir la misma búsqueda si ya está pintada
    if (q === _swapLastQuery && listDiv.children.length > 0) return;
    _swapLastQuery = q;

    listDiv.innerHTML = '<div style="padding:10px;">Cargando...</div>';
    try {
        const url = q
            ? `/tokens?search=${encodeURIComponent(q)}&limit=50`
            : `/tokens?limit=50`;

        const res = await fetch(url);
        const json = await res.json();
        const tokens = (json && json.data) ? json.data : [];

        listDiv.innerHTML = '';

        const allItem = document.createElement('div');
        allItem.className = 'dropdown-item';
        allItem.innerText = TRANSLATIONS[currentLang].all_tokens_star || "🌟 Todos";
        allItem.onclick = () => resetSwapFilter();
        listDiv.appendChild(allItem);

        tokens.forEach(t => {
            if (!t || !t.symbol) return;
            const item = document.createElement('div');
            item.className = 'dropdown-item';

            const logo = getProxyUrl(t.logo);
            item.innerHTML = `<img src="${logo}" loading="lazy" decoding="async"
                style="width:20px; height:20px; border-radius:50%; margin-right:8px;"
                onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"> ${t.symbol}`;

            item.onclick = () => selectSwapToken(t.symbol);
            listDiv.appendChild(item);
        });
    } catch (e) {
        console.error(e);
        listDiv.innerHTML = '<div style="padding:10px; color:red;">Error</div>';
    }
}

function filterDropdownList() {
    const input = document.getElementById('swapDropdownSearch');
    const q = input ? input.value : '';
    clearTimeout(swapSearchTimeout);
    swapSearchTimeout = setTimeout(() => updateSwapTokenList(q), 250);
}

async function populateSwapTokenList() {
    const input = document.getElementById('swapDropdownSearch');
    const q = input ? input.value : '';
    return updateSwapTokenList(q);
}


// Estado Global
let favorites = [];
let myWallets = [];
let walletAliases = {};

try {
    favorites = JSON.parse(localStorage.getItem('sora_favorites') || '[]');
    myWallets = JSON.parse(localStorage.getItem('sora_wallets') || '[]');
} catch (e) {
    localStorage.removeItem('sora_favorites');
    localStorage.removeItem('sora_wallets');
}

function refreshAliases() {
    walletAliases = {};
    myWallets.forEach(w => walletAliases[w.address] = w.name);
}
refreshAliases();

// Helper for consistent number formatting (1,000.0000)
function formatAmount(val) {
    if (val === undefined || val === null) return '0.0000';
    const num = parseFloat(val);
    if (isNaN(num)) return val;
    return num.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 4 });
}

function formatAddress(address) {
    if (!address) return '???';
    if (walletAliases[address]) {
        return `<span class="wallet-alias" title="${address}" style="color:#D00060; font-weight:bold;">${walletAliases[address]}</span>`;
    }
    return address.substring(0, 6) + '...' + address.substring(address.length - 4);
}

function saveWallet(address, name, type) {
    if (!address || !name) return false;
    myWallets = myWallets.filter(w => w.address !== address);
    myWallets.push({ address, name, type });
    localStorage.setItem('sora_wallets', JSON.stringify(myWallets));
    refreshAliases();
    if (document.getElementById('balance').classList.contains('active')) loadBalanceTab();
    return true;
}

function deleteWallet(address) {
    myWallets = myWallets.filter(w => w.address !== address);
    localStorage.setItem('sora_wallets', JSON.stringify(myWallets));
    refreshAliases();
    if (document.getElementById('balance').classList.contains('active')) loadBalanceTab();
}

let tokenPage = 1;
let tokenTotalPages = 1;
let searchTimeout;
let showFavoritesOnly = false;

function formatPrice(val) {
    const num = parseFloat(val);
    if (isNaN(num) || num === 0) return '<span class="currency-symbol">$</span><span class="price-value">0.00</span>';
    const symbol = '<span class="currency-symbol">$</span>';
    if (Math.abs(num) >= 0.001) {
        const formatted = num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 6 });
        return `${symbol}<span class="price-value">${formatted}</span>`;
    }
    let str = num.toFixed(30);
    const match = str.match(/^0\.0+/);
    if (!match) return `${symbol}<span class="price-value">${num.toFixed(6)}</span>`;
    const zeros = match[0].length - 2;
    const remaining = str.substring(match[0].length).substring(0, 4);
    return `${symbol}<span class="price-value">0.0<sub style="color:#6B7280">${zeros}</sub>${remaining}</span>`;
}

// Throttle socket events to prevent network flooding
let lastTransferUpdate = 0;
const TRANSFER_THROTTLE_MS = 500; // Max 2 updates per second

// Listen for batched transfers (anti-saturation)
socket.on('transfers-batch', (batch) => {
    // Subimos a 50 para que veas todo el movimiento reciente
    const MAX_VISUAL_ITEMS = 20;

    const tbody = document.getElementById('transferTable');
    if (!tbody) return;
    // NO renderizar en background: reduce CPU/red y evita "crasheos" del navegador
    if (document.hidden || !document.getElementById('transfers')?.classList.contains('active')) return;
    if (tbody.children.length > 0 && tbody.children[0].innerText.includes(TRANSLATIONS[currentLang].waiting_activity)) {
        tbody.innerHTML = '';
    }

    // Tomamos solo los últimos N elementos del batch para evitar saturar el navegador con peticiones de imágenes
    const recentItems = batch.slice(-MAX_VISUAL_ITEMS);

    // Invertimos para mostrar el más reciente arriba (si el batch es cronológico)
    // Recorremos el array procesado y lo insertamos arriba
    for (const d of recentItems) {
        const row = document.createElement('tr');
        const fromShort = formatAddress(d.from);
        const toShort = formatAddress(d.to);
        const logoSrc = getProxyUrl(d.logo);

        row.innerHTML = `
<td style="color:#6B7280; font-size:13px;">${d.time}</td>
<td style="font-family:monospace; font-size:12px;"><a href="#" onclick="openBlockModal('${d.block}'); return false;" style="color:#D0021B;">#${d.block}</a></td>
<td><span onclick="openWalletDetails('${d.from}')" class="${fromShort ? 'wallet-unsaved' : ''}">${fromShort}</span></td>
<td>
<div class="asset-row">
    <img src="${logoSrc}" width="32" height="32" loading="lazy" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
    <div>
        <b>${d.amount} ${d.symbol}</b><br>
        <span style="color:#10B981; font-size:11px;">$${d.usdValue}</span>
    </div>
</div>
</td>
<td style="color:#D1D5DB;">➜</td>
<td><span onclick="openWalletDetails('${d.to}')" class="${toShort ? 'wallet-unsaved' : ''}">${toShort}</span></td>
<td>
    <button class="btn-ghost" onclick="openTxModal('${d.hash}', '${d.extrinsic_id}')" style="font-size:11px; padding:2px 6px;">🔍 Ver</button>
</td>`;
        tbody.insertBefore(row, tbody.firstChild);
    }

    // Limpiamos la tabla para que no crezca infinitamente
    while (tbody.children.length > MAX_VISUAL_ITEMS) {
        tbody.removeChild(tbody.lastChild);
    }
});

let liquidityIconsLoaded = false;
async function loadLiquidityIcons() {
    if (liquidityIconsLoaded) return;
    try {
        const map = { 'XOR': 'btnPoolXor', 'XSTUSD': 'btnPoolXst', 'KUSD': 'btnPoolKusd' };
        for (const [symbol, btnId] of Object.entries(map)) {
            const res = await fetch(`/tokens?search=${symbol}&limit=5`); // Increased limit to ensure we find the exact match
            const json = await res.json();
            if (json.data && json.data.length > 0) {
                // Find exact match
                const token = json.data.find(t => t.symbol === symbol);
                if (token && token.logo) {
                    const img = document.querySelector(`#${btnId} img`);
                    if (img) img.src = getProxyUrl(token.logo);
                }
            }
        }
        liquidityIconsLoaded = true;
    } catch (e) { console.error("Error loading icons", e); }
}

function openTab(name) {
    document.querySelectorAll('.tab-content').forEach(d => {
        d.classList.remove('active');
        d.style.display = 'none';
    });
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    const content = document.getElementById(name);
    if (content) {
        content.classList.add('active');
        content.style.display = 'block';
    }
    const btn = document.getElementById('tab-' + name);
    if (btn) btn.classList.add('active');

    // Save current tab
    localStorage.setItem('sorametrics_current_tab', name);

    if (name === 'balance') loadBalanceTab();
    if (name === 'tokens') loadTokens();
    if (name === 'liquidity') { loadPools(); loadLiquidityIcons(); loadGlobalLiquidity(); }
    if (name === 'transfers') loadGlobalTransfers();
    if (name === 'swaps') loadGlobalSwaps();
    if (name === 'bridges') loadGlobalBridges();
    if (name === 'section-intelligence') loadSoraIntelligence();
}

async function loadWalletLiquidity() {
    const list = document.getElementById('wLiquidityList');
    list.innerHTML = `<div style="text-align:center; padding:20px; color:#999;">${TRANSLATIONS[currentLang].loading}</div>`;

    if (!currentDetailsAddr) return;

    try {
        let poolsData = [];

        // Check if unified view mode
        if (currentDetailsAddr === 'unified-view' && unifiedWalletAddresses.length > 0) {
            // Fetch liquidity from all wallets in parallel
            const promises = unifiedWalletAddresses.map(addr =>
                fetch(`/wallet/liquidity/${addr}`).then(r => r.ok ? r.json() : []).catch(() => [])
            );
            const results = await Promise.all(promises);

            // Combine and aggregate pools (sum values for same pool pairs)
            const poolMap = {};
            results.forEach(pools => {
                pools.forEach(pool => {
                    const key = `${pool.base.symbol}-${pool.target.symbol}`;
                    if (!poolMap[key]) {
                        poolMap[key] = {
                            base: pool.base,
                            target: pool.target,
                            amountBase: 0,
                            amountTarget: 0,
                            value: 0,
                            share: 0
                        };
                    }
                    poolMap[key].amountBase += pool.amountBase || 0;
                    poolMap[key].amountTarget += pool.amountTarget || 0;
                    poolMap[key].value += pool.value || 0;
                    poolMap[key].share += pool.share || 0;
                });
            });

            poolsData = Object.values(poolMap).sort((a, b) => b.value - a.value);
        } else {
            // Single wallet mode
            const res = await fetch(`/wallet/liquidity/${currentDetailsAddr}`);
            if (!res.ok) {
                const text = await res.text();
                throw new Error(`Status ${res.status}: ${text.substring(0, 50)}`);
            }
            poolsData = await res.json();
        }

        if (!poolsData || poolsData.length === 0) {
            list.innerHTML = `<div style="text-align:center; padding:20px; color:#999;">${TRANSLATIONS[currentLang].no_data}</div>`;
            return;
        }

        list.innerHTML = '';
        poolsData.forEach(pool => {
            const baseLogo = getProxyUrl(pool.base.logo);
            const targetLogo = getProxyUrl(pool.target.logo);
            list.innerHTML += `
                <div class="card" style="margin:0; padding:15px; border:1px solid var(--border-color); background:var(--bg-card); display:flex; align-items:center; justify-content:space-between;">
                    <div style="display:flex; align-items:center; gap:15px;">
                        <div style="display:flex; position:relative; width:50px;">
                            <img src="${baseLogo}" style="width:32px; height:32px; border-radius:50%; z-index:2; border:2px solid var(--bg-card);" onerror="this.onerror=null;this.style.display='none'">
                            <img src="${targetLogo}" style="width:32px; height:32px; border-radius:50%; position:absolute; left:20px; z-index:1;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
                        </div>
                        <div>
                            <div style="font-weight:bold; font-size:16px;">${pool.base.symbol}-${pool.target.symbol}</div>
                            <div style="font-size:12px; color:var(--text-secondary);">${(pool.share * 100).toFixed(4)}% Share</div>
                        </div>
                    </div>
                    <div style="text-align:right;">
                        <div style="font-weight:bold; color:#10B981; font-size:16px;">$${(pool.value || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                        <div style="font-size:12px; color:var(--text-secondary);">
                            ${formatAmount(pool.amountBase)} ${pool.base.symbol} <br>
                            ${formatAmount(pool.amountTarget)} ${pool.target.symbol}
                        </div>
                    </div>
                </div>
            `;
        });
    } catch (e) {
        console.error(e);
        list.innerHTML = `<div style="text-align:center; color:red; padding:20px;">Error: ${e.message}</div>`;
    }
}

function toggleFavorite(symbol) {
    if (favorites.includes(symbol)) favorites = favorites.filter(s => s !== symbol); else favorites.push(symbol);
    localStorage.setItem('sora_favorites', JSON.stringify(favorites));
    if (document.getElementById('tokens').classList.contains('active')) loadTokens();
}



// Listen for network stats (Sora Intelligence)
socket.on('new-block-stats', (stats) => {
    // Just re-fetch to respect current filter
    loadNetworkHeader();
});

let lastSwapUpdate = 0;
const SWAP_THROTTLE_MS = 500; // Max 2 updates per second

// Listen for batched swaps (anti-saturation)
socket.on('swaps-batch', (batch) => {
    // Subimos a 50 para ver todo el flujo
    const MAX_VISUAL_ITEMS = 20;

    const tbody = document.getElementById('swapTable');
    if (!tbody) return;
    // NO renderizar en background: reduce CPU/red y evita "crasheos" del navegador
    if (document.hidden || !document.getElementById('swaps')?.classList.contains('active')) return;

    // Process only the most recent items from batch to protect network
    // Aunque lleguen 100 swaps, solo pedimos las imágenes de los 8 visibles
    const recentItems = batch.slice(-MAX_VISUAL_ITEMS);

    for (const d of recentItems) {
        const row = document.createElement('tr');
        const isSaved = walletAliases[d.wallet];
        const nameClass = isSaved ? 'wallet-saved' : 'wallet-unsaved';
        const short = formatAddress(d.wallet);

        // Optimización de imágenes (vía proxy)
        const logoIn = getProxyUrl(d.in.logo);
        const logoOut = getProxyUrl(d.out.logo);

        row.innerHTML = `
<td style="color:#6B7280; font-size:11px;">${d.time}</td>
<td style="font-family:monospace; font-size:12px;"><a href="#" onclick="openBlockModal('${d.block}'); return false;" style="color:#D0021B;">#${d.block}</a></td>
<td>
<div class="asset-row" style="align-items:center; display:flex; gap:8px;">
    <img src="${logoIn}" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" loading="lazy" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
    <div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(d.in.amount)}</b> ${d.in.symbol}</div>
</div>
</td>
<td style="color:#D1D5DB; font-size:12px;">➜</td>
<td>
<div class="asset-row" style="align-items:center; display:flex; gap:8px;">
    <img src="${logoOut}" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" loading="lazy" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
    <div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(d.out.amount)}</b> ${d.out.symbol}</div>
</div>
</td>
<td style="font-size:11px;">
<span onclick="openWalletDetails('${d.wallet}')" class="${nameClass}">${short}</span>
<span onclick="copyToClipboard('${d.wallet}')" style="cursor:pointer; margin-left:4px;" title="Copiar">📋</span>
</td>
<td>
    <button class="btn-ghost" onclick="openTxModal('${d.hash}', '${d.extrinsic_id}')" style="font-size:11px; padding:2px 6px;">🔍 Ver</button>
</td>`;
        tbody.insertBefore(row, tbody.firstChild);
    }

    // Keep table limited
    while (tbody.children.length > MAX_VISUAL_ITEMS) tbody.removeChild(tbody.lastChild);
});


let groupWalletsMode = localStorage.getItem('sora_group_wallets') === 'true';
let lastUnifiedData = null;

function updateGroupWalletsUI() {
    const cb = document.getElementById('groupWalletsCheckbox');
    const track = document.getElementById('groupWalletsTrack');
    const knob = document.getElementById('groupWalletsKnob');
    if (cb && track && knob) {
        cb.checked = groupWalletsMode;
        if (groupWalletsMode) {
            track.style.background = '#10B981';
            knob.style.transform = 'translateX(16px)';
        } else {
            track.style.background = '#E5E7EB';
            knob.style.transform = 'translateX(0)';
        }
    }
}

function toggleGroupWallets() {
    groupWalletsMode = !groupWalletsMode;
    localStorage.setItem('sora_group_wallets', groupWalletsMode);
    updateGroupWalletsUI();
    loadBalanceTab();
}

async function loadBalanceTab() {
    updateGroupWalletsUI();
    const myContainer = document.getElementById('myWalletsList');
    const watchContainer = document.getElementById('watchWalletsList');

    // Show loading
    myContainer.innerHTML = `<div style="text-align:center; padding:20px; color:#999; grid-column: 1 / -1;">${TRANSLATIONS[currentLang].loading}</div>`;
    watchContainer.innerHTML = `<div style="text-align:center; padding:20px; color:#999; grid-column: 1 / -1;">${TRANSLATIONS[currentLang].loading}</div>`;

    if (myWallets.length === 0) {
        myContainer.innerHTML = `<div style="text-align:center; padding:20px; color:#999; grid-column: 1 / -1;">${TRANSLATIONS[currentLang].no_wallets_saved}</div>`;
        watchContainer.innerHTML = `<div style="text-align:center; padding:20px; color:#999; grid-column: 1 / -1;">${TRANSLATIONS[currentLang].not_watching_wallets}</div>`;
        document.getElementById('totalNetWorth').innerHTML = formatPrice(0);
        return;
    }

    // --- BULK FETCH ---
    const allAddresses = myWallets.map(w => w.address);
    const resultsMap = {};
    let grandTotal = 0;

    try {
        // Send all addresses to backend
        const res = await fetch('/balances', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ addresses: allAddresses })
        });
        const json = await res.json();
        if (json.result) {
            json.result.forEach(r => { resultsMap[r.address] = r; });
        }
    } catch (e) {
        console.error("Error loading balances:", e);
    }

    const myW = myWallets.filter(w => w.type === 'my');
    const watchW = myWallets.filter(w => w.type === 'watch');

    let myHtml = '';
    let watchHtml = '';

    // Calculate Grand Total first
    myW.forEach(w => {
        const data = resultsMap[w.address];
        if (data) grandTotal += data.totalUsd;
    });

    if (groupWalletsMode && myW.length > 0) {
        // --- UNIFIED VIEW ---
        let unifiedTokens = {};

        myW.forEach(w => {
            const data = resultsMap[w.address];
            if (!data || !data.tokens) return;
            data.tokens.forEach(t => {
                if (!unifiedTokens[t.assetId]) {
                    unifiedTokens[t.assetId] = {
                        symbol: t.symbol,
                        logo: t.logo,
                        assetId: t.assetId,
                        amount: 0,
                        usdValue: 0
                    };
                }
                unifiedTokens[t.assetId].amount += parseFloat(t.amount);
                unifiedTokens[t.assetId].usdValue += parseFloat(t.usdValue);
            });
        });

        const tokenArray = Object.values(unifiedTokens)
            .sort((a, b) => b.usdValue - a.usdValue)
            .map(t => ({ ...t, amount: formatAmount(t.amount) }));

        const unifiedWallet = {
            name: `Portafolio Unificado (${myW.length} Wallets)`,
            address: 'unified-view',
            type: 'my'
        };
        const unifiedData = { tokens: tokenArray, totalUsd: grandTotal };
        lastUnifiedData = unifiedData;

        myHtml = createWalletCard(unifiedWallet, unifiedData, true); // true = isUnified

    } else {
        // --- INDIVIDUAL VIEW ---
        myW.forEach(w => {
            const data = resultsMap[w.address] || { tokens: [], totalUsd: 0 };
            myHtml += createWalletCard(w, data);
        });
    }

    watchW.forEach(w => {
        const data = resultsMap[w.address] || { tokens: [], totalUsd: 0 };
        watchHtml += createWalletCard(w, data);
    });

    myContainer.innerHTML = myW.length === 0
        ? `<div style="text-align:center; padding:20px; color:#999; grid-column: 1 / -1;">${TRANSLATIONS[currentLang].no_wallets_saved}</div>`
        : myHtml;

    watchContainer.innerHTML = watchW.length === 0
        ? `<div style="text-align:center; padding:20px; color:#999; grid-column: 1 / -1;">${TRANSLATIONS[currentLang].not_watching_wallets}</div>`
        : watchHtml;

    document.getElementById('totalNetWorth').innerHTML = formatPrice(grandTotal);
}

async function fetchBalance(address) {
    try {
        const res = await fetch(`/balance/${address}`);
        const tokens = await res.json();
        const totalUsd = tokens.reduce((acc, t) => acc + parseFloat(t.usdValue || 0), 0);
        return { tokens, totalUsd };
    } catch (e) { return { tokens: [], totalUsd: 0 }; }
}


function createWalletCard(wallet, data, isUnified = false) {
    const topTokens = data.tokens.slice(0, 3).map(t => `<img src="${getProxyUrl(t.logo)}" loading="lazy" decoding="async" fetchpriority="low" title="${t.amount} ${t.symbol}" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'" style="width:20px; height:20px; border-radius:50%; margin-right:-5px; border:1px solid var(--bg-card); object-fit:contain;">`).join('');

    // Hide delete button if unified
    const deleteBtn = isUnified ? '' : `<button style="border:none; background:none; color:#EF4444; cursor:pointer; z-index:10;" onclick="event.stopPropagation(); deleteWallet('${wallet.address}')">🗑️</button>`;

    // Disable click for unified (or make it open a modal with breakdown? for now disable)
    const onClick = isUnified ? 'onclick="openUnifiedDetails()"' : `onclick="openWalletDetails('${wallet.address}')"`;
    const cursor = 'pointer';
    const addressDisplay = isUnified ? `<span style="font-size:11px; color:#10B981; font-weight:bold;">VISTA AGRUPADA</span>` : `<div style="font-size:11px; color:var(--text-secondary);">${wallet.address.substring(0, 6)}...${wallet.address.substring(wallet.address.length - 4)}</div>`;

    return `
<div class="card" style="margin:0; padding:15px; border:1px solid var(--border-color); transition: all 0.2s; cursor:${cursor}; background:var(--bg-card);" ${onClick} onmouseover="this.style.boxShadow=var(--shadow-hover)" onmouseout="this.style.boxShadow='none'">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
    <div>
        <h4 style="margin:0; color:var(--text-primary);">${wallet.name}</h4>
        ${addressDisplay}
    </div>
    ${deleteBtn}
</div>
<div style="margin-top:15px;"><span style="font-size:20px; font-weight:bold; color:#10B981;">${formatPrice(data.totalUsd)}</span></div>
<div style="margin-top:10px; display:flex; align-items:center; justify-content:space-between;">
    <div style="padding-left:5px;">${topTokens}</div>
    <span style="font-size:12px; color:var(--text-secondary);">${data.tokens.length} ${TRANSLATIONS[currentLang].assets}</span>
</div></div>`;
}

function openAddWalletModal() { document.getElementById('addWalletModal').style.display = 'flex'; }
function closeAddWalletModal() { document.getElementById('addWalletModal').style.display = 'none'; }

let addWalletMode = 'single';

function switchAddWalletMode(mode) {
    addWalletMode = mode;
    document.getElementById('mode-single').classList.toggle('active', mode === 'single');
    document.getElementById('mode-bulk').classList.toggle('active', mode === 'bulk');

    document.getElementById('add-wallet-single').style.display = mode === 'single' ? 'block' : 'none';
    document.getElementById('add-wallet-bulk').style.display = mode === 'bulk' ? 'block' : 'none';
}

function submitAddWallet() {
    const type = document.getElementById('newWalletType').value;

    if (addWalletMode === 'single') {
        const addr = document.getElementById('newWalletAddr').value.trim();
        const name = document.getElementById('newWalletName').value.trim();

        if (!addr || !name) {
            alert('Por favor completa todos los campos');
            return;
        }

        if (saveWallet(addr, name, type)) {
            closeAddWalletModal();
            // Limpiar
            document.getElementById('newWalletAddr').value = '';
            document.getElementById('newWalletName').value = '';
        }
    } else {
        // BULK MODE
        const fileInput = document.getElementById('bulkWalletFile');
        const file = fileInput.files[0];
        if (!file) {
            alert('Please select a file');
            return;
        }

        const reader = new FileReader();
        reader.onload = function (e) {
            const content = e.target.result;
            const lines = content.split(/\r?\n/);
            let addedCount = 0;
            let errorCount = 0;

            lines.forEach(line => {
                line = line.trim();
                if (!line) return;

                const parts = line.split(',');
                const addr = parts[0].trim();
                // Si la parte 2 existe, usala como nombre. Si no, usa el address abreviado
                const name = parts.length > 1 ? parts[1].trim() : `${addr.substring(0, 4)}...${addr.substring(addr.length - 4)}`;

                if (addr.startsWith('cn')) {
                    // Logic extracted from saveWallet but without triggering refresh
                    if (!addr || !name) return;
                    myWallets = myWallets.filter(w => w.address !== addr);
                    myWallets.push({ address: addr, name: name, type: type });
                    addedCount++;
                } else {
                    errorCount++;
                }
            });

            // Save once at the end
            if (addedCount > 0) {
                localStorage.setItem('sora_wallets', JSON.stringify(myWallets));
                refreshAliases();
                if (document.getElementById('balance').classList.contains('active')) loadBalanceTab();
            }

            alert(`Proceso completado.\nAñadidas: ${addedCount}\nErrores/Duplicados: ${errorCount}`);
            closeAddWalletModal();
            fileInput.value = ''; // Reset file input
        };
        reader.readAsText(file);
    }
}

function setTokenTab(mode) {
    showFavoritesOnly = (mode === 'fav');
    document.getElementById('btnTabAll').classList.toggle('active', !showFavoritesOnly);
    document.getElementById('btnTabFav').classList.toggle('active', showFavoritesOnly);
    document.getElementById('tokenSearch').style.display = showFavoritesOnly ? 'none' : 'block';
    tokenPage = 1;
    loadTokens();
}

// --- TOKENS LOGIC ---
let currentTimeframe = '24h';

function changeTimeframe(tf) {
    currentTimeframe = tf;
    // Update label
    const labelMap = { '1h': 'Última 1h', '4h': 'Últimas 4h', '24h': 'Últimas 24h', '7d': 'Últimos 7d' };
    const label = document.getElementById('tfLabel');
    if (label) label.innerText = labelMap[tf] || 'Últimas 24h';

    loadTokens();
}

async function loadTokens() {
    const tbody = document.getElementById('tokenTable');

    // UX Improvement: Non-destructive loading
    if (tbody.children.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding: 20px;">${TRANSLATIONS[currentLang].loading}</td></tr>`;
    } else {
        tbody.style.opacity = '0.5';
        tbody.style.pointerEvents = 'none'; // Prevent clicks while loading
    }

    const search = document.getElementById('tokenSearch').value;

    const btnRefresh = document.querySelector('button[onclick="loadTokens()"]');
    const originalText = btnRefresh ? btnRefresh.innerText : '';
    if (btnRefresh) {
        btnRefresh.disabled = true;
        btnRefresh.innerText = '⌛'; // Indicador visual
    }

    try {
        let url = `/tokens?page=${tokenPage}&limit=20&timeframe=${currentTimeframe}&sparkline=false`;
        if (showFavoritesOnly) {
            if (favorites.length === 0) {
                tbody.style.opacity = '1';
                tbody.style.pointerEvents = 'auto';
                tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;">${TRANSLATIONS[currentLang].no_favorites_yet}</td></tr>`;
                document.getElementById('tokenPageIndicator').innerText = '';
                document.getElementById('btnTokenPrev').disabled = true;
                document.getElementById('btnTokenNext').disabled = true;
                if (btnRefresh) { btnRefresh.disabled = false; btnRefresh.innerText = originalText; }
                return;
            }
            url += `&symbols=${favorites.join(',')}`;
        } else {
            url += `&search=${search}`;
        }

        const res = await fetch(url);
        const json = await res.json();
        const data = json.data;
        tokenTotalPages = json.totalPages;

        if (tokenTotalPages === 0 && showFavoritesOnly) {
            tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;">${TRANSLATIONS[currentLang].no_data}</td></tr>`;
        } else {
            const ind = document.getElementById('tokenPageIndicator');
            if (ind) ind.innerText = TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', tokenPage).replace('{total}', tokenTotalPages);
            const btnP = document.getElementById('btnTokenPrev');
            if (btnP) btnP.disabled = (tokenPage <= 1);
            const btnN = document.getElementById('btnTokenNext');
            if (btnN) btnN.disabled = (tokenPage >= tokenTotalPages);
        }

        // Optimización: Construir HTML en string para evitar reflows constantes
        let html = '';
        data.forEach(t => {
            const priceStr = t.price > 0 ? formatPrice(t.price) : '-';
            const isFav = favorites.includes(t.symbol);
            const starColor = isFav ? '#FFD700' : '#E5E7EB';

            let changeColor = '#6B7280'; // Gray by default
            let changeText = '-';
            if (t.change24h !== undefined && t.change24h !== null) {
                const val = parseFloat(t.change24h);
                if (val > 0) { changeColor = '#10B981'; changeText = '+' + val.toFixed(2) + '%'; }
                else if (val < 0) { changeColor = '#EF4444'; changeText = val.toFixed(2) + '%'; }
                else { changeText = '0.00%'; }
            }

            html += `<tr>
    <td style="cursor:pointer; font-size:18px;" onclick="toggleFavorite('${t.symbol}')"><span style="color:${starColor}">★</span></td>
    <td><div class="asset-row"><img src="${getProxyUrl(t.logo)}" loading="lazy" decoding="async" fetchpriority="low" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><div><b>${t.symbol}</b><br><span style="font-size:10px; color:#999;">${t.name}</span></div></div></td>
    <td>${priceStr}</td>
    <td style="color:${changeColor}; font-weight:500;">${changeText}</td>
    <td style="text-align: center;">
        <button class="btn-ghost" onclick="viewHolders('${t.symbol}', '${t.assetId}')" style="font-size:12px; padding: 4px 8px;">
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" style="vertical-align:text-bottom"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" /></svg>
            ${TRANSLATIONS[currentLang].holders}
        </button>
    </td>
    <td style="text-align: center;">
        <button class="btn-ghost js-show-chart" 
            data-symbol="${t.symbol}"
            style="font-size:12px; padding: 4px 8px; min-height:30px; min-width:30px;">
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" style="vertical-align:text-bottom; pointer-events:none;"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" /></svg>
            <span style="pointer-events:none;">${TRANSLATIONS[currentLang].chart}</span>
        </button>
    </td>
</tr>`;
        });
        tbody.innerHTML = html;
        tbody.style.opacity = '1';
        tbody.style.pointerEvents = 'auto';


    } catch (e) {
        tbody.style.opacity = '1';
        tbody.style.pointerEvents = 'auto';
        tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:red;">${TRANSLATIONS[currentLang].error_loading_data}</td></tr>`;
    } finally {
        if (btnRefresh) {
            btnRefresh.disabled = false;
            btnRefresh.innerText = TRANSLATIONS[currentLang].refresh;
        }
    }
}



function filterTokens() {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => { tokenPage = 1; loadTokens(); }, 800);
}

function changeTokenPage(d) {
    if (tokenPage + d > 0 && tokenPage + d <= tokenTotalPages) { tokenPage += d; loadTokens(); }
}

function viewHolders(symbol, assetId) {
    currentAssetId = assetId;
    currentPage = 1;
    document.getElementById('holderModal').style.display = 'flex';
    document.getElementById('modalTitle').innerText = `${TRANSLATIONS[currentLang].holders} de ${symbol}`;
    loadHoldersPage();
}

async function loadHoldersPage() {
    const tbody = document.getElementById('holdersTableBody');
    tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;">${TRANSLATIONS[currentLang].loading}</td></tr>`;
    try {
        const res = await fetch(`/holders/${currentAssetId}?page=${currentPage}`);
        const json = await res.json();
        totalPages = json.totalPages;
        document.getElementById('totalHoldersBadge').innerText = `${json.totalHolders} ${TRANSLATIONS[currentLang].accounts}`;
        document.getElementById('pageIndicator').innerText = TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', currentPage).replace('{total}', totalPages);
        tbody.innerHTML = '';
        const startRank = (currentPage - 1) * 25;
        json.data.forEach((h, index) => {
            const short = formatAddress(h.address);
            tbody.innerHTML += `<tr>
                <td>${startRank + index + 1}</td>
                <td><span class="clickable-address" onclick="openWalletDetails('${h.address}')" style="cursor:pointer; color:var(--text-primary); font-weight:bold;">${short}</span></td>
                <td style="text-align:right;">${h.balanceStr}</td>
            </tr>`;
        });
        document.getElementById('btnPrev').disabled = (currentPage <= 1);
        document.getElementById('btnNext').disabled = (currentPage >= totalPages);
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="3" style="text-align:center; color:red;">${TRANSLATIONS[currentLang].error_loading}</td></tr>`;
    }
}

function changePage(d) {
    if (currentPage + d > 0 && currentPage + d <= totalPages) { currentPage += d; loadHoldersPage(); }
}
function closeModal() { document.getElementById('holderModal').style.display = 'none'; }

// --- POOLS LOGIC ---
let poolPage = 1; let poolTotalPages = 1; let pFilter = 'all';
let transferPage = 1; let transferTotalPages = 1; let swapPage = 1; let swapTotalPages = 1;

function changeTransferPage(d) {
    if (transferPage + d > 0 && transferPage + d <= transferTotalPages) { transferPage += d; loadGlobalTransfers(); }
}
function changeSwapPage(d) {
    if (swapPage + d > 0 && swapPage + d <= swapTotalPages) { swapPage += d; loadGlobalSwaps(); }
}
function setPoolFilter(mode) {
    pFilter = mode; poolPage = 1;
    const btns = ['btnPoolAll', 'btnPoolXor', 'btnPoolXst', 'btnPoolKusd'];
    btns.forEach(id => document.getElementById(id).classList.remove('active'));
    if (mode === 'all') document.getElementById('btnPoolAll').classList.add('active');
    if (mode === 'XOR') document.getElementById('btnPoolXor').classList.add('active');
    if (mode === 'XSTUSD') document.getElementById('btnPoolXst').classList.add('active');
    if (mode === 'XSTUSD') document.getElementById('btnPoolXst').classList.add('active');
    if (mode === 'KUSD') document.getElementById('btnPoolKusd').classList.add('active');
    loadPools();
}

function changePoolPage(d) {
    if (poolPage + d > 0 && poolPage + d <= poolTotalPages) { poolPage += d; loadPools(); }
}

async function loadPools() {
    const tbody = document.getElementById('poolTable');
    tbody.innerHTML = `<tr><td colspan="3" style="text-align:center; padding:20px;">${TRANSLATIONS[currentLang].loading}</td></tr>`;
    try {
        const filter = pFilter || 'all';
        const url = `/pools?page=${poolPage}&limit=10&base=${filter}`;
        const res = await fetch(url);
        const json = await res.json();
        const data = json.data;
        poolTotalPages = json.totalPages || 1;
        poolTotalPages = json.totalPages || 1;
        const totalBadge = document.getElementById('poolTotalBadge');
        if (totalBadge) totalBadge.innerText = json.total || 0;

        const pageText = TRANSLATIONS[currentLang] ? TRANSLATIONS[currentLang].page_x_of_y : "Page {current} of {total}";
        const pageInd = document.getElementById('poolPageIndicator');
        if (pageInd) pageInd.innerText = pageText.replace('{current}', json.page || 1).replace('{total}', poolTotalPages);

        const btnPrev = document.getElementById('btnPoolPrev');
        if (btnPrev) btnPrev.disabled = (poolPage <= 1);

        const btnNext = document.getElementById('btnPoolNext');
        if (btnNext) btnNext.disabled = (poolPage >= poolTotalPages);

        tbody.innerHTML = '';
        if (data.length === 0) {
            const noDataText = TRANSLATIONS[currentLang] ? TRANSLATIONS[currentLang].no_data : "No data";
            tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;">${noDataText}</td></tr>`;
            return;
        }

        data.forEach(p => {
            const baseSymbol = p.base.symbol === '?' ? (p.base.assetId.substring(0, 6) + '...') : p.base.symbol;
            const targetSymbol = p.target.symbol === '?' ? (p.target.assetId.substring(0, 6) + '...') : p.target.symbol;
            const baseLogo = getProxyUrl(p.base.logo);
            const targetLogo = getProxyUrl(p.target.logo);

            // Defensive: Check if BigNumber is available
            let baseAmount, targetAmount;
            if (typeof BigNumber !== 'undefined') {
                baseAmount = new BigNumber(p.reserves.base.replace(/,/g, '')).div(new BigNumber(10).pow(p.base.decimals)).toFormat(2);
                targetAmount = new BigNumber(p.reserves.target.replace(/,/g, '')).div(new BigNumber(10).pow(p.target.decimals)).toFormat(2);
            } else {
                // Fallback: Simple division
                baseAmount = (parseFloat(p.reserves.base.replace(/,/g, '')) / Math.pow(10, p.base.decimals)).toLocaleString(undefined, { maximumFractionDigits: 2 });
                targetAmount = (parseFloat(p.reserves.target.replace(/,/g, '')) / Math.pow(10, p.target.decimals)).toLocaleString(undefined, { maximumFractionDigits: 2 });
            }
            // Calculate USD Value
            const rawBase = parseFloat(p.reserves.base.replace(/,/g, '')) / Math.pow(10, p.base.decimals);
            const rawTarget = parseFloat(p.reserves.target.replace(/,/g, '')) / Math.pow(10, p.target.decimals);
            const bPrice = p.basePrice || 0;
            const tPrice = p.targetPrice || 0;
            const tvl = (rawBase * bPrice) + (rawTarget * tPrice);
            const tvlStr = tvl > 0 ? `$${tvl.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : '-';

            tbody.innerHTML += `<tr>
                <td><div class="asset-row"><div style="display:flex; position:relative; width:50px; margin-right:10px;"><img src="${baseLogo}" style="width:32px; height:32px; border-radius:50%; z-index:2; border:2px solid white;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><img src="${targetLogo}" style="width:32px; height:32px; border-radius:50%; position:absolute; left:20px; z-index:1;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"></div><div><b>${baseSymbol}-${targetSymbol}</b></div></div></td>
                <td>
                    <div style="font-size:13px; color:var(--text-primary);">
                        <div>${baseAmount} <b>${baseSymbol}</b></div>
                        <div>${targetAmount} <b>${targetSymbol}</b></div>
                        <div style="margin-top:4px; font-weight:bold; color:#10B981;">Total: ${tvlStr}</div>
                    </div>
                </td>
                <td>
                    <button class="secondary-btn" onclick="openPoolDetails('${p.base.assetId}', '${p.target.assetId}', 'providers', '${baseSymbol}', '${targetSymbol}', '${baseLogo.replace(/'/g, "\\'")}', '${targetLogo.replace(/'/g, "\\'")}')">
                        Providers
                    </button>
                </td>
                <td style="text-align:right;">
                    <button class="secondary-btn" style="float:right;" onclick="openPoolDetails('${p.base.assetId}', '${p.target.assetId}', 'activity', '${baseSymbol}', '${targetSymbol}', '${baseLogo.replace(/'/g, "\\'")}', '${targetLogo.replace(/'/g, "\\'")}')">
                        Activity
                    </button>
                </td>
            </tr>`;
        });
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="4" style="text-align:center; color:red;">${TRANSLATIONS[currentLang].error_loading_pools}<br><small style="color:#999">${e.message}</small></td></tr>`;
    }
}

let currentDetailsAddr = null;
let wSwapPage = 1;
let wTransferPage = 1;

async function openWalletDetails(address) {
    currentDetailsAddr = address;
    wSwapPage = 1;
    wTransferPage = 1;
    document.getElementById('walletDetailsModal').style.display = 'flex';
    document.body.style.overflow = 'hidden';
    const isSaved = !!walletAliases[address];
    const name = walletAliases[address] || TRANSLATIONS[currentLang].wallet;

    let titleHtml = name;
    if (isSaved) {
        titleHtml += ` <button onclick="editWalletAlias('${address}')" title="Editar Alias" style="background:none; border:none; cursor:pointer; font-size:16px; margin-left:8px;">✏️</button>`;
    }
    document.getElementById('detailsTitle').innerHTML = titleHtml;
    document.getElementById('detailsAddr').innerText = address;
    openWTab('assets');
    loadWalletAssets();
    loadWalletHistory();
}

function editWalletAlias(address) {
    const currentName = walletAliases[address];
    const newName = prompt("Nuevo nombre para esta wallet:", currentName);

    if (newName && newName.trim() !== "") {
        // Update in memory
        const walletIndex = myWallets.findIndex(w => w.address === address);
        if (walletIndex >= 0) {
            myWallets[walletIndex].name = newName.trim(); // Update in place to keep type
            localStorage.setItem('sora_wallets', JSON.stringify(myWallets));
            refreshAliases();

            // Update UI
            if (document.getElementById('balance').classList.contains('active')) loadBalanceTab();

        }
    }
}
function closeDetailsModal() {
    document.getElementById('walletDetailsModal').style.display = 'none';
    document.body.style.overflow = '';
    // Clear unified addresses when closing
    unifiedWalletAddresses = [];
}

// Global variable to store unified wallet addresses for combined data loading
let unifiedWalletAddresses = [];

function openUnifiedDetails() {
    if (!lastUnifiedData) return;
    currentDetailsAddr = 'unified-view';

    // Store addresses of all "my" wallets for combined data loading
    unifiedWalletAddresses = myWallets.filter(w => w.type === 'my').map(w => w.address);

    document.getElementById('walletDetailsModal').style.display = 'flex';
    document.body.style.overflow = 'hidden';

    document.getElementById('detailsTitle').innerHTML = "Portafolio Unificado";
    document.getElementById('detailsAddr').innerText = `${unifiedWalletAddresses.length} Wallets`;

    // Show ALL tabs (not hiding them anymore)
    ['wtab-swaps', 'wtab-transfers', 'wtab-bridges', 'wtab-liquidity'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = '';
    });

    openWTab('assets');
    loadUnifiedAssets();
}

function loadUnifiedAssets() {
    const div = document.getElementById('wAssetsList');
    div.innerHTML = '';
    if (!lastUnifiedData || !lastUnifiedData.tokens || lastUnifiedData.tokens.length === 0) {
        div.innerHTML = TRANSLATIONS[currentLang].no_assets_found;
        return;
    }
    lastUnifiedData.tokens.forEach(t => {
        div.innerHTML += `<div class="token-card"><img src="${getProxyUrl(t.logo)}" loading="lazy" decoding="async" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'" style="width:40px; border-radius:50%;"><br><b>${t.amount} ${t.symbol}</b><br><span style="color:#10B981">${formatPrice(t.usdValue)}</span></div>`;
    });
}
function openWTab(tab) {
    ['assets', 'swaps', 'transfers', 'bridges', 'liquidity'].forEach(t => {
        document.getElementById('wtab-' + t)?.classList.toggle('active', t === tab);
        const view = document.getElementById('wview-' + t);
        if (view) view.style.display = t === tab ? 'block' : 'none';
    });
    if (tab === 'swaps') loadWalletSwaps();
    if (tab === 'transfers') loadWalletTransfers();
    if (tab === 'bridges') loadWalletBridges();
    if (tab === 'liquidity') loadWalletLiquidity();
}

let wBridgePage = 1;
function changeWBridgePage(delta) {
    wBridgePage += delta;
    if (wBridgePage < 1) wBridgePage = 1;
    loadWalletBridges();
}

async function loadWalletBridges() {
    const tbody = document.getElementById('wBridgesTable');
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;">Loading...</td></tr>';

    try {
        let bridges = [];
        let totalPages = 1;

        // Check if unified view mode
        if (currentDetailsAddr === 'unified-view' && unifiedWalletAddresses.length > 0) {
            // Fetch bridges from all wallets in parallel
            const promises = unifiedWalletAddresses.map(addr =>
                fetch(`/history/bridges/${addr}?page=1&limit=50`).then(r => r.json()).catch(() => ({ data: [] }))
            );
            const results = await Promise.all(promises);

            // Combine all bridges (avoiding duplicates)
            const seen = new Set();
            results.forEach(json => {
                if (json.data) {
                    json.data.forEach(tx => {
                        const key = `${tx.block}-${tx.sender}-${tx.recipient}-${tx.amount}`;
                        if (!seen.has(key)) {
                            seen.add(key);
                            bridges.push(tx);
                        }
                    });
                }
            });

            // Sort by timestamp descending
            bridges.sort((a, b) => new Date(b.time) - new Date(a.time));
            bridges = bridges.slice(0, 30); // Show top 30 combined
            totalPages = 1;
        } else {
            // Single wallet mode
            const response = await fetch(`/history/bridges/${currentDetailsAddr}?page=${wBridgePage}&limit=10`);
            const result = await response.json();
            bridges = result.data || [];
            totalPages = result.totalPages || 1;
        }

        tbody.innerHTML = '';
        if (bridges.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding:20px; color:#999;">No bridge history found.</td></tr>';
            return;
        }

        bridges.forEach(tx => {
            const row = document.createElement('tr');
            const color = tx.direction === 'Incoming' ? '#10B981' : '#EF4444';
            const arrow = tx.direction === 'Incoming' ? '⬇' : '⬆';

            // Asset Info is now provided by backend
            const symbol = tx.symbol || 'UNK';

            row.innerHTML = `
                <td style="font-size:12px; color:var(--text-secondary);">${tx.time.split(' ')[0]} <span style="color:#999;">${tx.time.split(' ')[1]}</span></td>
                <td>
                    <div style="display:flex; align-items:center; gap:5px;">
                        <span style="font-weight:600; color:var(--text-primary);">${symbol}</span>
                    </div>
                </td>
                <td style="font-weight:600; color:${color};">${arrow} ${formatAmount(tx.amount)}</td>
                <td>
                    <div style="font-size:12px;">
                        <span style="display:block; font-weight:600; color:var(--text-primary);">${tx.network}</span>
                        <span style="color:var(--text-secondary); font-size:10px;">${tx.recipient.substring(0, 6)}...</span>
                    </div>
                </td>
            `;
            tbody.appendChild(row);
        });

        document.getElementById('wBridgePageIndicator').innerText = `${TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', wBridgePage).replace('{total}', totalPages)}`;
        document.getElementById('btnWBridgePrev').disabled = wBridgePage <= 1;
        document.getElementById('btnWBridgeNext').disabled = wBridgePage >= totalPages;

    } catch (e) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color:red;">Error loading data</td></tr>';
        console.error(e);
    }
}

async function loadWalletAssets() {
    const div = document.getElementById('wAssetsList');
    div.innerHTML = TRANSLATIONS[currentLang].loading;
    const data = await fetchBalance(currentDetailsAddr);
    div.innerHTML = '';
    if (data.tokens.length === 0) { div.innerHTML = TRANSLATIONS[currentLang].no_assets_found; return; }
    data.tokens.forEach(t => {
        div.innerHTML += `<div class="token-card"><img src="${getProxyUrl(t.logo)}" loading="lazy" decoding="async" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'" style="width:40px; border-radius:50%;"><br><b>${formatAmount(t.amount)} ${t.symbol}</b><br><span style="color:#10B981">${formatPrice(t.usdValue)}</span></div>`;
    });
}

async function loadWalletHistory() {
    await Promise.all([loadWalletSwaps(), loadWalletTransfers()]);
}

async function loadWalletSwaps() {
    const sBody = document.getElementById('wSwapsTable');
    sBody.innerHTML = `<tr><td colspan="3" style="text-align:center;">${TRANSLATIONS[currentLang].loading}</td></tr>`;

    try {
        let swaps = [];
        let totalPages = 1;

        // Check if unified view mode
        if (currentDetailsAddr === 'unified-view' && unifiedWalletAddresses.length > 0) {
            // Fetch swaps from all wallets in parallel
            const promises = unifiedWalletAddresses.map(addr =>
                fetch(`/history/swaps/${addr}?page=1&limit=100`).then(r => r.json()).catch(() => ({ data: [] }))
            );
            const results = await Promise.all(promises);

            // Combine all swaps
            results.forEach(json => {
                if (json.data) swaps.push(...json.data);
            });

            // Sort by timestamp descending and limit
            swaps.sort((a, b) => new Date(b.time) - new Date(a.time));
            swaps = swaps.slice(0, 50); // Show top 50 combined
            totalPages = 1; // No pagination for unified view
        } else {
            // Single wallet mode
            const sRes = await fetch(`/history/swaps/${currentDetailsAddr}?page=${wSwapPage}&limit=25`);
            const json = await sRes.json();
            swaps = json.data;
            totalPages = json.totalPages || 1;
        }

        // Update Controls
        document.getElementById('wSwapPageIndicator').innerText = `Page ${wSwapPage} of ${totalPages}`;
        document.getElementById('btnWSwapPrev').disabled = wSwapPage <= 1;
        document.getElementById('btnWSwapNext').disabled = wSwapPage >= totalPages;

        sBody.innerHTML = '';
        if (swaps.length === 0) { sBody.innerHTML = `<tr><td colspan="3" style="text-align:center;">${TRANSLATIONS[currentLang].no_recent_swaps}</td></tr>`; return; }
        swaps.forEach(s => {
            sBody.innerHTML += `<tr><td style="color:#6B7280; font-size:11px;">${s.time}</td>
            <td><div class="asset-row" style="align-items:center; display:flex; gap:8px;"><img src="${getProxyUrl(s.in.logo)}" loading="lazy" decoding="async" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(s.in.amount)}</b> ${s.in.symbol}</div></div></td>
            <td><div class="asset-row" style="align-items:center; display:flex; gap:8px;"><img src="${getProxyUrl(s.out.logo)}" loading="lazy" decoding="async" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(s.out.amount)}</b> ${s.out.symbol}</div></div></td></tr>`;
        });
    } catch (e) {
        console.error('Error loading swaps:', e);
        sBody.innerHTML = `<tr><td colspan="3" style="color:red; text-align:center;">Error: ${e.message}</td></tr>`;
    }
}

async function loadWalletTransfers() {
    const tBody = document.getElementById('wTransfersTable');
    tBody.innerHTML = `<tr><td colspan="4" style="text-align:center;">${TRANSLATIONS[currentLang].loading}</td></tr>`;

    try {
        let transfers = [];
        let totalPages = 1;

        // Check if unified view mode
        if (currentDetailsAddr === 'unified-view' && unifiedWalletAddresses.length > 0) {
            // Fetch transfers from all wallets in parallel
            const promises = unifiedWalletAddresses.map(addr =>
                fetch(`/history/transfers/${addr}?page=1&limit=100`).then(r => r.json()).catch(() => ({ data: [] }))
            );
            const results = await Promise.all(promises);

            // Combine all transfers (avoiding duplicates by using a Set based on key)
            const seen = new Set();
            results.forEach(json => {
                if (json.data) {
                    json.data.forEach(t => {
                        const key = `${t.block}-${t.from}-${t.to}-${t.amount}`;
                        if (!seen.has(key)) {
                            seen.add(key);
                            transfers.push(t);
                        }
                    });
                }
            });

            // Sort by timestamp descending and limit
            transfers.sort((a, b) => new Date(b.time) - new Date(a.time));
            transfers = transfers.slice(0, 50); // Show top 50 combined
            totalPages = 1; // No pagination for unified view
        } else {
            // Single wallet mode
            const tRes = await fetch(`/history/transfers/${currentDetailsAddr}?page=${wTransferPage}&limit=25`);
            const json = await tRes.json();
            transfers = json.data;
            totalPages = json.totalPages || 1;
        }

        // Update Controls
        document.getElementById('wTransferPageIndicator').innerText = `Page ${wTransferPage} of ${totalPages}`;
        document.getElementById('btnWTransferPrev').disabled = wTransferPage <= 1;
        document.getElementById('btnWTransferNext').disabled = wTransferPage >= totalPages;

        tBody.innerHTML = '';
        if (transfers.length === 0) { tBody.innerHTML = `<tr><td colspan="4" style="text-align:center;">${TRANSLATIONS[currentLang].no_recent_transfers}</td></tr>`; return; }

        transfers.forEach(t => {
            // For unified view, check if to/from is any of our wallets
            const isIn = currentDetailsAddr === 'unified-view'
                ? unifiedWalletAddresses.includes(t.to) && !unifiedWalletAddresses.includes(t.from)
                : t.to === currentDetailsAddr;
            const type = isIn ? `<span style="color:green; font-weight:bold;">⬇ ${TRANSLATIONS[currentLang].received}</span>` : `<span style="color:red; font-weight:bold;">⬆ ${TRANSLATIONS[currentLang].sent}</span>`;
            const other = isIn ? t.from : t.to;
            const otherShort = formatAddress(other);
            const isSavedOther = walletAliases[other];
            const otherClass = isSavedOther ? 'wallet-saved' : 'wallet-unsaved';
            tBody.innerHTML += `<tr><td style="color:#6B7280; font-size:11px;">${t.time}</td><td style="font-size:12px;">${type}</td><td style="font-size:11px;"><span onclick="openWalletDetails('${other}')" class="${otherClass}">${otherShort}</span><span onclick="copyToClipboard('${other}')" style="cursor:pointer; margin-left:4px;" title="Copiar">📋</span></td><td><div class="asset-row" style="align-items:center; display:flex; gap:8px;"><img src="${getProxyUrl(t.logo)}" loading="lazy" decoding="async" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(t.amount)} ${t.symbol}</b><br><span style="color:#10B981; font-size:10px;">$${t.usdValue}</span></div></div></td></tr>`;
        });
    } catch (e) {
        console.error('Error loading transfers:', e);
        tBody.innerHTML = `<tr><td colspan="4" style="color:red; text-align:center;">Error: ${e.message}</td></tr>`;
    }
}

function changeWSwapPage(delta) {
    wSwapPage += delta;
    if (wSwapPage < 1) wSwapPage = 1;
    loadWalletSwaps();
}

function changeWTransferPage(delta) {
    wTransferPage += delta;
    if (wTransferPage < 1) wTransferPage = 1;
    loadWalletTransfers();
}

// --- SORA INTELLIGENCE FRONTEND LOGIC (GLOBAL SCOPE) ---

// --- CUSTOMIZABLE NAVIGATION LOGIC ---

const allSections = [
    { id: 'balance', name: 'Balance', icon: '💰', translateKey: 'tab_balance' },
    { id: 'swaps', name: 'Swaps', icon: '🔄', translateKey: 'tab_swaps' },
    { id: 'transfers', name: 'Transferencias', icon: '💸', translateKey: 'tab_transfers' },
    { id: 'tokens', name: 'Tokens', icon: '🪙', translateKey: 'tab_tokens' },
    { id: 'bridges', name: 'Puentes', icon: '🌉', translateKey: 'tab_bridges' },
    { id: 'liquidity', name: 'Liquidez', icon: '🌊', translateKey: 'tab_liquidity' },
    { id: 'section-intelligence', name: 'Estadísticas de Red', icon: '🧠', translateKey: 'network_stats_btn' }
];

// Default actives
const defaultActives = ['balance', 'swaps', 'transfers', 'tokens', 'liquidity'];
let activeSectionIds = JSON.parse(localStorage.getItem('sorametrics_active_tabs')) || defaultActives;

function initNavigation() {
    renderTabs();
    renderSidebar();

    // Restore last tab
    const lastTab = localStorage.getItem('sorametrics_current_tab') || 'balance';
    if (activeSectionIds.includes(lastTab)) {
        openTab(lastTab);
    } else if (activeSectionIds.length > 0) {
        openTab(activeSectionIds[0]);
    }
}

function renderTabs() {
    const container = document.getElementById('dynamicTabsContainer');
    if (!container) return;
    container.innerHTML = '';

    activeSectionIds.forEach(id => {
        const sec = allSections.find(s => s.id === id);
        if (!sec) return;

        const btn = document.createElement('button');
        btn.className = 'tab-btn';
        btn.id = 'tab-' + id;

        // Use translation key if available
        let btnText = sec.name;
        if (sec.translateKey && TRANSLATIONS[currentLang] && TRANSLATIONS[currentLang][sec.translateKey]) {
            btnText = TRANSLATIONS[currentLang][sec.translateKey];
        }

        btn.innerText = btnText;
        btn.onclick = () => openTab(id);
        container.appendChild(btn);
    });
}

function renderSidebar() {
    const list = document.getElementById('sidebarSectionList');
    if (!list) return;
    list.innerHTML = '';

    allSections.forEach(sec => {
        const isActive = activeSectionIds.includes(sec.id);
        const item = document.createElement('div');
        item.style.cssText = "display:flex; justify-content:space-between; align-items:center; padding:10px; border-bottom:1px solid #f3f4f6;";

        // Use translation key if available
        let sectionName = sec.name;
        if (sec.translateKey && TRANSLATIONS[currentLang] && TRANSLATIONS[currentLang][sec.translateKey]) {
            sectionName = TRANSLATIONS[currentLang][sec.translateKey];
        }

        // Checkbox creation
        item.innerHTML = `
            <div style="display:flex; align-items:center; gap:10px; color:#374151;">
                <span style="font-size:16px;">${sec.icon}</span>
                <span style="font-size:14px; font-weight:500;">${sectionName}</span>
            </div>
            <label class="switch" style="position: relative; display: inline-block; width: 34px; height: 20px;">
                <input type="checkbox" ${isActive ? 'checked' : ''} style="opacity: 0; width: 0; height: 0;">
                <span class="slider round" style="position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #ccc; transition: .4s; border-radius: 34px;"></span>
            </label>
        `;

        // Add event listener manually to avoid string escaping formatting hell
        const checkbox = item.querySelector('input');
        checkbox.addEventListener('change', (e) => toggleSection(sec.id, e.target));

        // Slider internal style injection for 'checked' state requires CSS class or inline manipulation logic
        // For simplicity, we'll use a helper class in CSS, but let's try to handle color changes via JS for self-containment if CSS isn't present
        checkbox.addEventListener('change', (e) => {
            const slider = item.querySelector('.slider');
            slider.style.backgroundColor = e.target.checked ? '#D0021B' : '#ccc';
        });
        // Init state
        const slider = item.querySelector('.slider');
        slider.style.backgroundColor = isActive ? '#D0021B' : '#ccc';

        // Slider knob
        slider.innerHTML = `<span class="knob" style="position: absolute; content: ''; height: 12px; width: 12px; left: 4px; bottom: 4px; background-color: white; transition: .4s; border-radius: 50%; transform: ${isActive ? 'translateX(14px)' : 'translateX(0)'};"></span>`;

        checkbox.addEventListener('change', (e) => {
            const knob = item.querySelector('.knob');
            knob.style.transform = e.target.checked ? 'translateX(14px)' : 'translateX(0)';
        });

        list.appendChild(item);
    });
}

function toggleSection(id, checkbox) {
    if (checkbox.checked) {
        if (activeSectionIds.length >= 5) {
            checkbox.checked = false;
            // Revert visuals
            const slider = checkbox.nextElementSibling;
            slider.style.backgroundColor = '#ccc';
            slider.querySelector('.knob').style.transform = 'translateX(0)';

            alert("Máximo 5 secciones permitidas. Desactiva una primero.");
            return;
        }
        activeSectionIds.push(id);
    } else {
        activeSectionIds = activeSectionIds.filter(x => x !== id);
    }

    // Persist
    localStorage.setItem('sorametrics_active_tabs', JSON.stringify(activeSectionIds));

    // Re-render
    renderTabs();

    // Handle visibility of current tab
    const currentTab = localStorage.getItem('sorametrics_current_tab');
    if (!activeSectionIds.includes(currentTab)) {
        // If we hid the active tab, switch to another
        if (activeSectionIds.length > 0) {
            openTab(activeSectionIds[0]);
        } else {
            // No tabs visible? Hide all content
            document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
            document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
        }
    }
}

function toggleMenu() {
    const menu = document.getElementById('sideMenu');
    const overlay = document.getElementById('menuOverlay');
    menu.classList.toggle('show');
    overlay.classList.toggle('show');
}

// Note: openTab is defined near line 450. initNavigation is called from DOMContentLoaded.

let intelTimeframe = '1d';

function setIntelTimeframe(tf, btn) {
    intelTimeframe = tf;
    // Update active button
    if (btn) {
        btn.parentElement.querySelectorAll('.tab-btn-small').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
    }
    loadAccumulation(); // Reload chart
}

async function loadSoraIntelligence() {
    loadNetworkHeader();
    loadAccumulation();
    loadNetworkHealth();
}

async function loadNetworkHeader() {
    try {
        const tfEl = document.getElementById('header-timeframe');
        const tf = tfEl ? tfEl.value : '1d';

        const res = await fetch(`/stats/header?timeframe=${tf}`);
        const stats = await res.json();

        const update = (id, val) => {
            const el = document.getElementById(id);
            if (el) el.innerText = val ? val.toLocaleString() : '0';
        };

        if (stats.block) document.getElementById('stat-block').innerText = '#' + stats.block.toLocaleString();

        // Removed Extrinsics
        update('stat-swaps', stats.swaps);
        update('stat-transfers', stats.transfers);
        update('stat-bridges', stats.bridges);

        const lbl = tf === 'all' ? 'All Time' : (tf === '1d' ? 'Past 24h' : `Past ${tf}`);
        ['lbl-swaps', 'lbl-transfers', 'lbl-bridges'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerText = lbl;
        });

        loadNetworkFees();

    } catch (e) { console.error('Stats header error:', e); }
}

async function loadNetworkFees() {
    try {
        const tfEl = document.getElementById('header-timeframe');
        const tf = tfEl ? tfEl.value : '1d';

        const res = await fetch(`/stats/fees?timeframe=${tf}`);
        const data = await res.json();

        const map = { Swap: { xor: 0, usd: 0 }, Transfer: { xor: 0, usd: 0 }, Bridge: { xor: 0, usd: 0 }, Other: { xor: 0, usd: 0 } };
        let totalXor = 0, totalUsd = 0;

        data.forEach(row => {
            const t = map[row.type] ? row.type : 'Other';
            map[t].xor += row.total_xor || 0;
            map[t].usd += row.total_usd || 0;
            totalXor += row.total_xor || 0;
            totalUsd += row.total_usd || 0;
        });

        const setVal = (id, val, isUsd) => {
            const el = document.getElementById(id);
            if (el) el.innerText = isUsd
                ? '$' + val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                : val.toLocaleString(undefined, { maximumFractionDigits: 4 }) + ' XOR';
        };

        setVal('fee-swap-xor', map.Swap.xor, false);
        setVal('fee-swap-usd', map.Swap.usd, true);
        setVal('fee-transfer-xor', map.Transfer.xor, false);
        setVal('fee-transfer-usd', map.Transfer.usd, true);
        setVal('fee-bridge-xor', map.Bridge.xor, false);
        setVal('fee-bridge-usd', map.Bridge.usd, true);
        setVal('fee-other-xor', map.Other.xor, false);
        setVal('fee-other-usd', map.Other.usd, true);

        setVal('fee-total-xor', totalXor, false);
        setVal('fee-total-usd', totalUsd, true);

        // Render Charts
        renderFeeCharts(map);

    } catch (e) { console.error('Fees error:', e); }
}

// Global Chart Instances
var feeDonutChart = null;
var feeLineChart = null;

async function renderFeeCharts(currentMap) {
    // VISIBLE DEBUG (DELETE LATER)
    if (typeof Chart === 'undefined') {
        console.warn('Chart.js not ready, retrying in 500ms...');
        setTimeout(() => renderFeeCharts(currentMap), 500);
        return;
    }

    const titles = TRANSLATIONS[currentLang] || TRANSLATIONS['es'];

    // 1. DONUT CHART (Distribution)
    const ctxDonut = document.getElementById('feesDonutChart');

    if (ctxDonut) {
        try {
            const labels = ['Swap', 'Transfer', 'Bridge', 'Other'];
            const data = [
                currentMap.Swap.xor,
                currentMap.Transfer.xor,
                currentMap.Bridge.xor,
                currentMap.Other.xor
            ];

            if (feeDonutChart) {
                feeDonutChart.data.datasets[0].data = data;
                feeDonutChart.options.plugins.title.text = TRANSLATIONS[currentLang].fees_pie_title || 'XOR Burned by Type';
                feeDonutChart.update();
            } else {
                const existing = Chart.getChart(ctxDonut);
                if (existing) existing.destroy();

                feeDonutChart = new Chart(ctxDonut, {
                    type: 'doughnut',
                    data: {
                        labels: labels,
                        datasets: [{
                            data: data,
                            backgroundColor: ['#D0021B', '#10B981', '#3B82F6', '#6B7280'],
                            borderWidth: 0
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { position: 'left' },
                            title: { display: true, text: TRANSLATIONS[currentLang].fees_pie_title || 'XOR Burned by Type' }
                        },
                        cutout: '70%'
                    }
                });
            }
        } catch (e) { console.error('DONUT ERROR:', e); }
    }

    // 2. LINE CHART (Trend) - Now in USD for better visual stability
    const ctxLine = document.getElementById('feesLineChart');
    if (ctxLine) {
        try {
            const tfEl = document.getElementById('header-timeframe');
            const tf = tfEl ? tfEl.value : '1d';

            // Fetch specific trend data
            const res = await fetch(`/stats/fees/trend?timeframe=${tf}`);
            const trendData = await res.json();

            const labels = trendData.map(d => d.bucket); // Time string
            const values = trendData.map(d => d.total_usd);

            if (feeLineChart) {
                feeLineChart.data.labels = labels;
                feeLineChart.data.datasets[0].data = values;
                feeLineChart.options.plugins.title.text = TRANSLATIONS[currentLang].fees_line_title || 'Activity Trend (Fees)';
                feeLineChart.update();
            } else {
                const existing = Chart.getChart(ctxLine);
                if (existing) existing.destroy();

                feeLineChart = new Chart(ctxLine, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'Fees Paid (USD)',
                            data: values,
                            borderColor: '#D0021B',
                            backgroundColor: 'rgba(208, 2, 27, 0.1)',
                            fill: true,
                            tension: 0.4,
                            borderWidth: 2
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false },
                            title: { display: true, text: titles.fees_line_title || 'Activity Trend (Fees)' },
                            tooltip: {
                                callbacks: {
                                    label: function (context) {
                                        return '$' + context.parsed.y.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                                    }
                                }
                            }
                        },
                        scales: {
                            x: { display: false }, // Hide x labels to avoid clutter
                            y: {
                                beginAtZero: true,
                                ticks: {
                                    callback: function (value) {
                                        return '$' + value.toLocaleString();
                                    }
                                }
                            }
                        }
                    }
                });
            }
        } catch (e) { console.error('LINE ERROR:', e); }
    }
}

// Whale Accumulation Pagination
let whalePage = 1;
let whaleTotalPages = 1;
let whaleData = [];

async function loadAccumulation() {
    const symbol = document.getElementById('intel-acc-token').value;
    const tfEl = document.getElementById('header-timeframe');
    const tf = tfEl ? tfEl.value : '1d';
    const t = TRANSLATIONS[currentLang] || TRANSLATIONS['es'];

    const container = document.getElementById('intel-acc-list');
    container.innerHTML = `<div style="text-align:center; padding:20px; color:#999;">⌛ ${t.loading_whales || 'Loading...'} (${symbol})</div>`;

    try {
        const res = await fetch(`/stats/accumulation?symbol=${symbol}&timeframe=${tf}`);
        const json = await res.json();

        if (!json.data || json.data.length === 0) {
            container.innerHTML = `<div style="text-align:center; padding:20px; color:#999;">${t.no_data || 'No data.'}</div>`;
            return;
        }

        // Store data and calculate pages
        whaleData = json.data;
        whaleTotalPages = Math.ceil(whaleData.length / 5);
        whalePage = 1;

        renderWhales();

    } catch (e) {
        console.error(e);
        container.innerHTML = `<div style="color:#ef4444; padding:20px;">Error cargando datos.</div>`;
    }
}

function renderWhales() {
    const container = document.getElementById('intel-acc-list');
    const symbol = document.getElementById('intel-acc-token').value;
    const t = TRANSLATIONS[currentLang] || TRANSLATIONS['es'];

    if (!whaleData || whaleData.length === 0) {
        container.innerHTML = `<div style="text-align:center; padding:20px; color:#999;">${t.no_data || 'No data.'}</div>`;
        return;
    }

    const startIndex = (whalePage - 1) * 5;
    const endIndex = startIndex + 5;
    const pageData = whaleData.slice(startIndex, endIndex);

    let html = '';
    pageData.forEach((w, index) => {
        const rank = startIndex + index + 1;
        const percentage = w.last_buy ? new Date(w.last_buy).toLocaleDateString() : '-';
        const alias = walletAliases[w.wallet] || formatAddress(w.wallet);
        const isWhale = w.total_bought_usd > 50000;
        const icon = isWhale ? '🐋' : (w.total_bought_usd > 10000 ? '🦈' : '🐟');

        // Make alias clickable
        const aliasHtml = `<span onclick="openWalletDetails('${w.wallet}')" style="cursor:pointer; border-bottom:1px dotted #999;" title="Ver detalles">${alias}</span>`;

        html += `
        <div class="whale-row">
            <div style="flex:1;">
                <div style="font-weight:600; font-size:13px;">${rank}. ${icon} ${aliasHtml}</div>
                <div style="font-size:11px; color:#999;">${w.swap_count} buys • Last: ${percentage}</div>
            </div>
            <div style="text-align:right;">
                <div style="font-weight:bold; color:#10B981;">+${parseFloat(w.total_bought_amount).toLocaleString()} ${symbol}</div>
                <div style="font-size:11px; color:#6B7280;">$${w.total_bought_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
            </div>
        </div>`;
    });

    // Add pagination controls
    const pageText = (t.page_x_of_y || 'Page {current} of {total}').replace('{current}', whalePage).replace('{total}', whaleTotalPages);
    html += `
    <div style="display:flex; justify-content:space-between; align-items:center; margin-top:15px; padding-top:10px; border-top:1px solid var(--border-color);">
        <button class="secondary-btn" onclick="changeWhalePage(-1)" ${whalePage <= 1 ? 'disabled style="opacity:0.5;"' : ''}>⬅ ${t.previous || 'Previous'}</button>
        <span style="font-size:12px; color:#6B7280;">${pageText}</span>
        <button class="top-btn" onclick="changeWhalePage(1)" ${whalePage >= whaleTotalPages ? 'disabled style="opacity:0.5;"' : ''}>${t.next || 'Next'} ➡</button>
    </div>`;

    container.innerHTML = html;
}

function changeWhalePage(delta) {
    whalePage += delta;
    if (whalePage < 1) whalePage = 1;
    if (whalePage > whaleTotalPages) whalePage = whaleTotalPages;
    renderWhales();
}


async function loadNetworkInfo() {
    try {
        const tfEl = document.getElementById('header-timeframe');
        const tf = tfEl ? tfEl.value : '1d';
        const res = await fetch(`/stats/overview?timeframe=${tf}`);
        const json = await res.json();

        // 1. Network Info
        if (json.network) {
            // Active Accounts
            const accountsEl = document.getElementById('intel-accounts');
            if (accountsEl) accountsEl.innerHTML = json.network.users?.toLocaleString() || '0';

            // Swap Volume in KUSD
            const swapVolEl = document.getElementById('intel-swap-vol');
            if (swapVolEl) swapVolEl.innerHTML = `$${(json.network.volume || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

            // LP Volume (new liquidity)
            const lpVolEl = document.getElementById('intel-lp-vol');
            if (lpVolEl) {
                const vol = json.network.lpVolume || 0;
                lpVolEl.innerHTML = `$${vol.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
                lpVolEl.style.color = '#F59E0B'; // Match chart color
            }

            // Transfer Volume
            const transferVolEl = document.getElementById('intel-transfer-vol');
            if (transferVolEl) transferVolEl.innerHTML = `$${(json.network.transferVolume || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
        }

        // 2. Pegs - REMOVED: Handled by loadStablecoinMonitor() now

        // 3. Trends (Old list removed, now Chart)
        // Fetch Trend Data
        try {
            const trendRes = await fetch(`/stats/network/trend?timeframe=${tf}`);
            const trendJson = await trendRes.json();
            console.log("Trend Data:", trendJson);

            const ctxNet = document.getElementById('networkLineChart');
            if (ctxNet) {
                // 1. Collect all unique buckets
                const allBuckets = new Set();
                [trendJson.swaps, trendJson.transfers, trendJson.lp, trendJson.accounts].forEach(arr => {
                    arr.forEach(d => allBuckets.add(d.bucket));
                });

                // If no data, show nothing or empty chart
                let labelsStr = Array.from(allBuckets).sort();

                // If completely empty, maybe add today's bucket so chart renders as empty grid
                if (labelsStr.length === 0) {
                    const now = new Date().toISOString().slice(0, 13) + ":00:00"; // simplistic
                    labelsStr = [now];
                }

                // 2. Map formatted labels
                const labels = labelsStr.map(b => {
                    const date = new Date(b);
                    return tf === '1d' ? date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : date.toLocaleDateString();
                });

                // 3. Map Data to Labels (fill 0 if missing)
                const getVal = (arr, bucket) => {
                    const found = arr.find(d => d.bucket === bucket);
                    return found && found.val > 0 ? found.val : null;
                };

                const swapData = labelsStr.map(b => getVal(trendJson.swaps, b));
                const transferData = labelsStr.map(b => getVal(trendJson.transfers, b));
                const lpData = labelsStr.map(b => getVal(trendJson.lp, b));
                const accData = labelsStr.map(b => getVal(trendJson.accounts, b));

                const existingChart = Chart.getChart(ctxNet);
                if (existingChart) existingChart.destroy();

                new Chart(ctxNet, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Swap Vol ($)',
                                data: swapData,
                                borderColor: '#8B5CF6',
                                backgroundColor: 'rgba(139, 92, 246, 0.1)',
                                yAxisID: 'y',
                                tension: 0.4,
                                fill: true,
                                spanGaps: true
                            },
                            {
                                label: 'Transfer Vol ($)',
                                data: transferData,
                                borderColor: '#10B981',
                                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                                yAxisID: 'y',
                                tension: 0.4,
                                fill: true,
                                spanGaps: true
                            },
                            {
                                label: 'New LP ($)',
                                data: lpData,
                                borderColor: '#F59E0B',
                                borderDash: [5, 5],
                                yAxisID: 'y',
                                tension: 0.4,
                                fill: false,
                                spanGaps: true
                            },
                            {
                                label: 'Active Accounts',
                                data: accData,
                                borderColor: '#3B82F6',
                                yAxisID: 'y1',
                                tension: 0.4,
                                borderWidth: 2,
                                pointRadius: 3,
                                spanGaps: true
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: {
                            mode: 'index',
                            intersect: false,
                        },
                        plugins: {
                            title: { display: false },
                            legend: { position: 'top', labels: { boxWidth: 10, usePointStyle: true } }
                        },
                        scales: {
                            x: { display: true, grid: { display: false } },
                            y: {
                                type: 'logarithmic',
                                display: true,
                                position: 'left',
                                title: { display: true, text: 'Volume ($) - Log Scale' },
                                grid: { color: '#333' },
                                min: 10
                            },
                            y1: {
                                type: 'linear',
                                display: true,
                                position: 'right',
                                title: { display: true, text: 'Active Users' },
                                grid: { drawOnChartArea: false }
                            }
                        }
                    }
                });
            }

        } catch (e) { console.error("Network Trend Chart Error:", e); }



    } catch (e) {
        console.error("Network Info error", e);
    }
}

// Backward compatibility alias
function loadNetworkHealth() {
    loadNetworkInfo();
}


async function loadGlobalSwaps(reset = false) {
    if (reset) swapPage = 1;
    const tbody = document.getElementById('swapTable');
    tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding:20px;">${TRANSLATIONS[currentLang].loading}</td></tr>`;

    // Date Filter
    const dateInput = document.getElementById('swapDateInput');
    const timestamp = dateInput && dateInput.value ? new Date(dateInput.value).getTime() : null;

    try {
        let url = `/history/global/swaps?page=${swapPage}&limit=25`;
        if (currentSwapFilter) url += `&filter=${encodeURIComponent(currentSwapFilter)}`;
        if (timestamp) url += `&timestamp=${timestamp}`;

        const res = await fetch(url);
        const json = await res.json();
        const data = json.data;
        swapTotalPages = json.totalPages;
        document.getElementById('swapPageIndicator').innerText = TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', swapPage).replace('{total}', swapTotalPages || 1);
        document.getElementById('btnSwapPrev').disabled = (swapPage <= 1);
        document.getElementById('btnSwapNext').disabled = (swapPage >= swapTotalPages);
        document.getElementById('btnSwapFirst').disabled = (swapPage <= 1);
        document.getElementById('btnSwapLast').disabled = (swapPage >= swapTotalPages);

        tbody.innerHTML = '';
        if (data.length === 0) { tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding:20px; color:#9CA3AF;">${TRANSLATIONS[currentLang].no_swaps_found}</td></tr>`; return; }
        data.forEach(d => {
            const short = formatAddress(d.wallet);
            const isSaved = walletAliases[d.wallet];
            const nameClass = isSaved ? 'wallet-saved' : 'wallet-unsaved';
            const row = document.createElement('tr');
            row.innerHTML = `<td style="color:#6B7280; font-size:11px;">${d.time}</td>
            <td style="font-family:monospace; font-size:12px;"><a href="#" onclick="openBlockModal('${d.block}'); return false;" style="color:#D0021B;">#${d.block}</a></td>
            <td><div class="asset-row" style="align-items:center; display:flex; gap:8px;"><img src="${getProxyUrl(d.in.logo)}" loading="lazy" decoding="async" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(d.in.amount)}</b> ${d.in.symbol}<br><span style="font-size:10px; color:#9CA3AF;">$${d.in.usd}</span></div></div></td><td style="color:#D1D5DB; font-size:12px;">➜</td><td><div class="asset-row" style="align-items:center; display:flex; gap:8px;"><img src="${getProxyUrl(d.out.logo)}" loading="lazy" decoding="async" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(d.out.amount)}</b> ${d.out.symbol}<br><span style="font-size:10px; color:#9CA3AF;">$${d.out.usd}</span></div></div></td><td style="font-size:11px;"><span onclick="openWalletDetails('${d.wallet}')" class="${nameClass}">${short}</span><span onclick="copyToClipboard('${d.wallet}')" style="cursor:pointer; margin-left:4px;" title="Copiar">📋</span></td>
            <td>
                <button class="btn-ghost" onclick="openTxModal('${d.hash}', '${d.extrinsic_id}')" style="font-size:11px; padding:2px 6px;">🔍 Ver</button>
            </td>`;
            tbody.appendChild(row);
        });
    } catch (e) { tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding:20px; color:red;">${TRANSLATIONS[currentLang].error_loading}</td></tr>`; }
}

function changeSwapPage(delta) {
    if (delta === 'first') swapPage = 1;
    else if (delta === 'last') swapPage = swapTotalPages;
    else swapPage += delta;

    if (swapPage < 1) swapPage = 1;
    if (swapPage > swapTotalPages) swapPage = swapTotalPages;
    loadGlobalSwaps();
}

function copyToClipboard(text) {
    if (navigator.clipboard) { navigator.clipboard.writeText(text).then(() => { alert('Dirección copiada: ' + text); }); }
    else { const ta = document.createElement('textarea'); ta.value = text; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); alert('Dirección copiada: ' + text); }
}

async function loadGlobalTransfers(reset = false) {
    if (reset) transferPage = 1;
    const tbody = document.getElementById('transferTable');
    tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding:20px;">${TRANSLATIONS[currentLang].loading}</td></tr>`;

    // Date Filter
    const dateInput = document.getElementById('transferDateInput');
    const timestamp = dateInput && dateInput.value ? new Date(dateInput.value).getTime() : null;

    try {
        let url = `/history/global/transfers?page=${transferPage}&limit=25`;
        if (timestamp) url += `&timestamp=${timestamp}`;

        const res = await fetch(url);
        const json = await res.json();
        const data = json.data;
        transferTotalPages = json.totalPages;
        document.getElementById('transferPageIndicator').innerText = TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', transferPage).replace('{total}', transferTotalPages || 1);
        document.getElementById('btnTransferPrev').disabled = (transferPage <= 1);
        document.getElementById('btnTransferNext').disabled = (transferPage >= transferTotalPages);
        document.getElementById('btnTransferFirst').disabled = (transferPage <= 1);
        document.getElementById('btnTransferLast').disabled = (transferPage >= transferTotalPages);

        tbody.innerHTML = '';
        if (data.length === 0) { tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding:20px; color:#9CA3AF;">${TRANSLATIONS[currentLang].waiting_activity}</td></tr>`; return; }
        data.forEach(d => {
            const fromShort = formatAddress(d.from);
            const toShort = formatAddress(d.to);
            const isSavedFrom = walletAliases[d.from];
            const fromClass = isSavedFrom ? 'wallet-saved' : 'wallet-unsaved';
            const isSavedTo = walletAliases[d.to];
            const toClass = isSavedTo ? 'wallet-saved' : 'wallet-unsaved';
            const row = document.createElement('tr');
            row.innerHTML = `<td style="color:#6B7280; font-size:11px;">${d.time}</td>
            <td style="font-family:monospace; font-size:12px;"><a href="#" onclick="openBlockModal('${d.block}'); return false;" style="color:#D0021B;">#${d.block}</a></td>
            <td style="font-size:11px;"><span onclick="openWalletDetails('${d.from}')" class="${fromClass}">${fromShort}</span><span onclick="copyToClipboard('${d.from}')" style="cursor:pointer; margin-left:4px;" title="Copiar">📋</span></td><td><div class="asset-row" style="align-items:center; display:flex; gap:8px;"><img src="${getProxyUrl(d.logo)}" loading="lazy" decoding="async" style="width:23px; height:23px; border-radius:50%; margin-right:5px; object-fit:contain;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'"><div style="font-size:11px;"><b style="font-size:14px;">${formatAmount(d.amount)} ${d.symbol}</b><br><span style="color:#10B981; font-size:10px;">$${d.usdValue}</span></div></div></td><td style="color:#D1D5DB;">➜</td><td style="font-size:11px;"><span onclick="openWalletDetails('${d.to}')" class="${toClass}">${toShort}</span><span onclick="copyToClipboard('${d.to}')" style="cursor:pointer; margin-left:4px;" title="Copiar">📋</span></td>
            <td>
                <button class="btn-ghost" onclick="openTxModal('${d.hash}', '${d.extrinsic_id}')" style="font-size:11px; padding:2px 6px;">🔍 Ver</button>
            </td>`;
            tbody.appendChild(row);
        });
    } catch (e) { tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding:20px; color:red;">${TRANSLATIONS[currentLang].error_loading}</td></tr>`; }
}

function changeTransferPage(delta) {
    if (delta === 'first') transferPage = 1;
    else if (delta === 'last') transferPage = transferTotalPages;
    else transferPage += delta;

    if (transferPage < 1) transferPage = 1;
    if (transferPage > transferTotalPages) transferPage = transferTotalPages;
    loadGlobalTransfers();
}

// --- BRIDGES TAB ---
let bridgePage = 1;
let bridgeTotalPages = 1;

async function loadGlobalBridges(reset = false) {
    if (reset) bridgePage = 1;
    const tbody = document.getElementById('bridgeTable');
    if (!tbody) return;
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; padding:20px;">${TRANSLATIONS[currentLang].loading}</td></tr>`;

    // Date Filter
    const dateInput = document.getElementById('bridgeDateInput');
    const timestamp = dateInput && dateInput.value ? new Date(dateInput.value).getTime() : null;

    try {
        let url = `/history/global/bridges?page=${bridgePage}&limit=25`;
        if (timestamp) url += `&timestamp=${timestamp}`;

        const res = await fetch(url);
        const json = await res.json();
        const data = json.data;
        bridgeTotalPages = json.totalPages || 1;
        document.getElementById('bridgePageIndicator').innerText = TRANSLATIONS[currentLang].page_x_of_y.replace('{current}', bridgePage).replace('{total}', bridgeTotalPages);
        document.getElementById('btnBridgePrev').disabled = (bridgePage <= 1);
        document.getElementById('btnBridgeNext').disabled = (bridgePage >= bridgeTotalPages);
        document.getElementById('btnBridgeFirst').disabled = (bridgePage <= 1);
        document.getElementById('btnBridgeLast').disabled = (bridgePage >= bridgeTotalPages);

        tbody.innerHTML = '';
        if (!data || data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; padding:20px; color:#9CA3AF;">${TRANSLATIONS[currentLang].no_data || 'No data'}</td></tr>`;
            return;
        }

        data.forEach(d => {
            const senderShort = formatAddress(d.sender);
            const recipientShort = formatAddress(d.recipient);
            const directionIcon = d.direction === 'Outgoing' ? '⬆ OUT' : '⬇ IN';
            const directionColor = d.direction === 'Outgoing' ? '#EF4444' : '#10B981';

            const row = document.createElement('tr');
            row.innerHTML = `
                <td style="color:#6B7280; font-size:11px;">${d.time || new Date(d.timestamp).toLocaleString()}</td>
                <td style="font-family:monospace; font-size:12px;"><a href="#" onclick="openBlockModal('${d.block}'); return false;" style="color:#D0021B;">#${d.block}</a></td>
                <td style="font-size:12px;">${d.network || 'Ethereum'}</td>
                <td style="color:${directionColor}; font-weight:600; font-size:11px;">${directionIcon}</td>
                <td style="font-size:11px;">
                    ${(() => {
                    const isEthAddress = d.sender && d.sender.startsWith('0x');
                    const isNullAddress = d.sender === '0x0000000000000000000000000000000000000000' || d.sender === 'Ethereum' || !d.sender;
                    const isIncoming = d.direction === 'Incoming';

                    if (isIncoming && isNullAddress) {
                        // Unknown origin - show Bridge Contract (clickable to view tx details)
                        return `<a href="#" onclick="openTxModal('${d.hash}', '${d.extrinsic_id}'); return false;" style="color:#627EEA; text-decoration:none; font-weight:500;" title="Ver detalles de transacción">🌉 ${TRANSLATIONS[currentLang].bridge_contract || 'Bridge Contract'} 🔗</a>`;
                    } else if (isEthAddress) {
                        // Real ETH address - link to Etherscan
                        return `<a href="https://etherscan.io/address/${d.sender}" target="_blank" style="color:#627EEA; text-decoration:none;" title="${d.sender}">${senderShort} 🔗</a>`;
                    } else {
                        // SORA address
                        return `<span onclick="openWalletDetails('${d.sender}')" class="wallet-unsaved">${senderShort}</span>`;
                    }
                })()}
                    <span onclick="copyToClipboard('${d.sender}')" style="cursor:pointer; margin-left:4px;" title="Copiar">📋</span>
                </td>
                <td style="font-size:11px;">
                    ${(() => {
                    const isEthAddress = d.recipient && d.recipient.startsWith('0x');
                    if (isEthAddress) {
                        // ETH address - link to Etherscan
                        return `<a href="https://etherscan.io/address/${d.recipient}" target="_blank" style="color:#627EEA; text-decoration:none;" title="${d.recipient}">${recipientShort} 🔗</a>`;
                    } else {
                        // SORA address
                        return `<span onclick="openWalletDetails('${d.recipient}')" class="wallet-unsaved">${recipientShort}</span>`;
                    }
                })()}
                    <span onclick="copyToClipboard('${d.recipient}')" style="cursor:pointer; margin-left:4px;" title="Copiar">📋</span>
                </td>
                <td>
                    <div class="asset-row" style="align-items:center; display:flex; gap:8px;">
                        <img src="${getProxyUrl(d.logo)}" loading="lazy" decoding="async" style="width:23px; height:23px; border-radius:50%; object-fit:contain;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
                        <div style="font-size:11px;">
                            <b style="font-size:14px;">${formatAmount(d.amount)} ${d.symbol || 'UNK'}</b><br>
                            <span style="color:#10B981; font-size:10px;">$${Number(d.usd_value || 0).toFixed(2)}</span>
                        </div>
                    </div>
                </td>
                <td>
                    <button class="btn-ghost" onclick="openTxModal('${d.hash}', '${d.extrinsic_id}')" style="font-size:11px; padding:2px 6px;">🔍 ${TRANSLATIONS[currentLang].view || 'View'}</button>
                </td>
            `;
            tbody.appendChild(row);
        });
    } catch (e) {
        console.error('Error loading bridges:', e);
        tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; padding:20px; color:red;">${TRANSLATIONS[currentLang].error_loading}</td></tr>`;
    }
}

function changeBridgePage(delta) {
    if (delta === 'first') bridgePage = 1;
    else if (delta === 'last') bridgePage = bridgeTotalPages;
    else bridgePage += delta;

    if (bridgePage < 1) bridgePage = 1;
    if (bridgePage > bridgeTotalPages) bridgePage = bridgeTotalPages;
    loadGlobalBridges();
}

function openBlockModal(block) {
    document.getElementById('blockModal').style.display = 'flex';
    document.getElementById('blockModalNumber').innerText = block;
    document.getElementById('blockModalContent').innerHTML = `
        <div style="text-align:center; padding:20px;">
            <p style="font-size:18px;">Block Height: <b>${block}</b></p>
            <p>
                <a href="https://sora.subscan.io/block/${block}" target="_blank" class="btn-primary" style="text-decoration:none; display:inline-block; margin-top:10px;">
                    View on Subscan ➜
                </a>
            </p>
        </div>
    `;
}

function openTxModal(hash, extrinsic_id) {
    document.getElementById('txModal').style.display = 'flex';

    // Detect if this is an Ethereum transaction (from incoming bridges)
    const isEthereum = extrinsic_id === 'ETH';
    const hasHash = hash && hash !== 'N/A' && hash !== '';
    const hasExtrinsicId = extrinsic_id && extrinsic_id !== 'N/A' && extrinsic_id !== '' && extrinsic_id !== 'ETH';

    // Build the link button
    let linkButton = '';
    if (isEthereum && hasHash) {
        // For incoming bridges, the hash is an internal SORA bridge request ID, not valid on Etherscan
        linkButton = `
            <span style="color:#6B7280; font-size:12px; display:block; text-align:center;">
                🌉 ${TRANSLATIONS[currentLang].bridge_internal_hash || 'This hash is an internal SORA bridge ID. It is not visible on Etherscan.'}
            </span>
        `;
    } else if (hasExtrinsicId) {
        linkButton = `
            <a href="https://sora.subscan.io/extrinsic/${extrinsic_id}" target="_blank" class="btn-primary" style="text-decoration:none; display:inline-block;">
                ${TRANSLATIONS[currentLang].view_on_subscan || 'View on Subscan'} ➜
            </a>
        `;
    }

    document.getElementById('txModalContent').innerHTML = `
        <div style="padding:10px;">
            <div style="margin-bottom:15px;">
                <label style="display:block; color:#6B7280; font-size:12px; margin-bottom:4px;">
                    ${isEthereum ? (TRANSLATIONS[currentLang].ethereum_request_hash || 'Ethereum Request Hash') : (TRANSLATIONS[currentLang].transaction_hash || 'Transaction Hash')}
                </label>
                <div style="background:#F3F4F6; padding:8px; border-radius:6px; font-family:monospace; word-break:break-all; font-size:13px; border:1px solid #E5E7EB; color:#374151;">
                    ${hasHash ? hash : 'N/A'}
                </div>
            </div>
            <div style="margin-bottom:20px;">
                <label style="display:block; color:#6B7280; font-size:12px; margin-bottom:4px;">
                    ${isEthereum ? (TRANSLATIONS[currentLang].origin || 'Origin') : (TRANSLATIONS[currentLang].extrinsic_id || 'Extrinsic ID')}
                </label>
                <div style="background:#F3F4F6; padding:8px; border-radius:6px; font-family:monospace; word-break:break-all; font-size:13px; border:1px solid #E5E7EB; color:#374151;">
                    ${isEthereum ? '🌐 ' + (TRANSLATIONS[currentLang].ethereum_network || 'Ethereum Network') : (hasExtrinsicId ? extrinsic_id : 'N/A')}
                </div>
            </div>
            <div style="text-align:center;">
                ${linkButton || '<span style="color:#6B7280;">' + (TRANSLATIONS[currentLang].no_external_link || 'No external link available') + '</span>'}
            </div>
        </div>
    `;
}

/* --- BACKUP SYSTEM --- */
function openBackupModal() {
    const modal = document.getElementById('backupModal');
    if (modal) {
        modal.style.display = 'flex';
        setTimeout(() => modal.style.opacity = '1', 10);
    }
}

function closeBackupModal() {
    const modal = document.getElementById('backupModal');
    if (modal) {
        modal.style.opacity = '0';
        setTimeout(() => modal.style.display = 'none', 300);
    }
}

window.addEventListener('click', (event) => {
    const modal = document.getElementById('backupModal');
    if (event.target === modal) closeBackupModal();
});

function exportBackup() {
    try {
        const data = {
            wallets: localStorage.getItem('sora_wallets'),
            favorites: localStorage.getItem('sora_favorites'),
            lang: localStorage.getItem('sora_lang'),
            timestamp: new Date().toISOString()
        };
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `sorametrics_backup_${new Date().toISOString().slice(0, 10)}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (e) { alert('Error: ' + e.message); }
}

function importBackup(input) {
    const file = input.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            const data = JSON.parse(e.target.result);
            if (!data.wallets && !data.favorites) { alert('Archivo inválido.'); return; }
            if (confirm('¿Restaurar backup?\nSe reemplazarán tus carteras y favoritos actuales.')) {
                if (data.wallets) localStorage.setItem('sora_wallets', data.wallets);
                if (data.favorites) localStorage.setItem('sora_favorites', data.favorites);
                if (data.lang) localStorage.setItem('sora_lang', data.lang);
                alert('¡Restaurado correctamente!');
                location.reload();
            }
        } catch (err) { alert('Error al leer archivo: ' + err.message); }
    };
    reader.readAsText(file);
    input.value = '';
}

// Global Orchestrator triggered by Sticky Header Selector
async function loadSoraIntelligence() {
    console.log('Refreshing Global Data (Sora Intelligence)...');

    // 1. Header (Swaps/Transfers/Bridges)
    if (typeof loadNetworkHeader === 'function') await loadNetworkHeader();

    // 2. Fees (Donut/Line)
    if (typeof loadNetworkFees === 'function') await loadNetworkFees();

    // 3. Accumulation (Whales)
    if (typeof loadAccumulation === 'function') await loadAccumulation();

    // 4. Health/Overview (Trends)
    if (typeof loadNetworkHealth === 'function') await loadNetworkHealth();

    // 5. Trending Tokens (Donut)
    if (typeof loadTrendingTokens === 'function') await loadTrendingTokens();

    // 6. Stablecoin Monitor (List + Chart)
    if (typeof loadStablecoinMonitor === 'function') await loadStablecoinMonitor();
}

/* CUSTOM DROPDOWN LOGIC */
function toggleGlobalDropdown(e) {
    e.stopPropagation(); // prevent window click
    const wrapper = document.getElementById('global-tf-wrapper');
    wrapper.classList.toggle('open');
}

function selectGlobalTimeframe(value, label) {
    // 1. Update Hidden Input (Source of Truth)
    document.getElementById('header-timeframe').value = value;

    // 2. Update Display
    document.getElementById('global-tf-display').innerText = label;

    // 3. Update Selected Class
    const options = document.querySelectorAll('.custom-option');
    options.forEach(opt => {
        if (opt.innerText === label) opt.classList.add('selected');
        else opt.classList.remove('selected');
    });

    // 4. Close Dropdown (defer slightly for visual feel)
    const wrapper = document.getElementById('global-tf-wrapper');
    setTimeout(() => wrapper.classList.remove('open'), 100);

    // 5. Trigger Refresh
    loadSoraIntelligence();
}

// Close dropdown when clicking outside
window.addEventListener('click', () => {
    const wrapper = document.getElementById('global-tf-wrapper');
    if (wrapper) wrapper.classList.remove('open');
});

/* --- THEME SYSTEM --- */
function toggleTheme() {
    const html = document.documentElement;
    const isDark = html.getAttribute('data-theme') === 'dark';
    const newTheme = isDark ? 'light' : 'dark';

    html.setAttribute('data-theme', newTheme);
    localStorage.setItem('sora_theme', newTheme);
    updateThemeIcon(newTheme);
    updateChartTheme(newTheme);
}

function updateThemeIcon(theme) {
    const sun = document.getElementById('icon-sun');
    const moon = document.getElementById('icon-moon');
    if (theme === 'dark') {
        if (sun) sun.style.display = 'none';
        if (moon) moon.style.display = 'block';
    } else {
        if (sun) sun.style.display = 'block';
        if (moon) moon.style.display = 'none';
    }
}

function initTheme() {
    const saved = localStorage.getItem('sora_theme') || 'light';
    document.documentElement.setAttribute('data-theme', saved);
    updateThemeIcon(saved);
}

function updateChartTheme(theme) {
    if (typeof Chart === 'undefined') return;

    // Determine colors
    const isDark = theme === 'dark';
    const textColor = isDark ? '#9ca3af' : '#6B7280';
    const gridColor = isDark ? '#333333' : '#E5E7EB';

    // Update Global Defaults
    Chart.defaults.color = textColor;
    Chart.defaults.borderColor = gridColor;
    if (Chart.defaults.scale) {
        Chart.defaults.scale.grid.color = gridColor;
    }

    // Trigger re-render of fees charts if they exist
    if (typeof renderFeeCharts === 'function') {
        // Simple re-call will use new colors because renderFeeCharts checks CSS vars or we can force it
        // Actually renderFeeCharts uses TRANSLATION strings but for colors it might need help
        // ideally renderFeeCharts should read getComputedStyle for colors
        // For now, let's just re-call it.
        renderFeeCharts();
    }
}


/* CUSTOM ACCUMULATION DROPDOWN */
function initAccDropdown() {
    console.log("Initializing Accumulation Dropdown with Favorites...");
    const wrapper = document.getElementById('acc-token-wrapper');
    if (!wrapper) return;

    const container = wrapper.querySelector('.custom-options');
    if (!container) return;

    // Base Tokens
    const baseTokens = ['XOR', 'VAL', 'PSWAP', 'TBCD', 'XSTUSD', 'KEN', 'KUSD', 'DAI'];

    // Merge with Favorites
    let favs = [];
    try {
        favs = JSON.parse(localStorage.getItem('sora_favorites') || '[]');
    } catch (e) {
        favs = [];
    }

    // Unique Set
    const uniqueTokens = [...new Set([...baseTokens, ...favs])];

    // Current Selection
    const currentVal = document.getElementById('intel-acc-token').value;

    // Generate HTML
    let html = '';
    uniqueTokens.forEach(sym => {
        const isSelected = sym === currentVal ? 'selected' : '';
        html += `<span class="custom-option ${isSelected}" onclick="selectAccToken('${sym}', '${sym}')">${sym}</span>`;
    });

    container.innerHTML = html;
}

function toggleAccDropdown(e) {
    e.stopPropagation();
    const wrapper = document.getElementById('acc-token-wrapper');
    wrapper.classList.toggle('open');
}

function selectAccToken(value, label) {
    // 1. Update Hidden Input
    document.getElementById('intel-acc-token').value = value;

    // 2. Update Display
    document.getElementById('acc-token-display').innerText = label;

    // 3. Update Selected Class
    const wrapper = document.getElementById('acc-token-wrapper');
    const options = wrapper.querySelectorAll('.custom-option');
    options.forEach(opt => {
        if (opt.innerText === label) opt.classList.add('selected');
        else opt.classList.remove('selected');
    });

    // 4. Close Dropdown
    setTimeout(() => wrapper.classList.remove('open'), 100);

    // 5. Trigger Reload
    loadAccumulation();
}

// Close dropdown when clicking outside (combine with existing listener if possible, else append)
window.addEventListener('click', () => {
    const wrapper = document.getElementById('acc-token-wrapper');
    if (wrapper) wrapper.classList.remove('open');
});

// Init on load
// Init on load
document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    initAccDropdown(); // Init favorites in dropdown
});

// --- POOL DETAILS MODAL LOGIC ---
window.currentPoolBase = null;
window.currentPoolTarget = null;
window.currentBaseSym = '';
window.currentTargetSym = '';

function openPoolDetails(base, target, initialTab, baseSym, targetSym, baseLogo, targetLogo) {
    window.currentPoolBase = base;
    window.currentPoolTarget = target;
    window.currentBaseSym = baseSym;
    window.currentTargetSym = targetSym;

    document.getElementById('poolDetailsModal').style.display = 'flex';

    // Header with Logos
    const hHtml = `
        <div style="display:flex; align-items:center; gap:12px;">
            <div style="display:flex; position:relative; width:50px;">
                <img src="${baseLogo}" style="width:30px; height:30px; border-radius:50%; z-index:2; border:2px solid var(--bg-card);" onerror="this.style.display='none'">
                <img src="${targetLogo}" style="width:30px; height:30px; border-radius:50%; position:absolute; left:20px; z-index:1;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
            </div>
            <div>
                <div style="font-size:20px; font-weight:bold;">${baseSym}-${targetSym}</div>
                <div style="font-size:12px; color:var(--text-secondary); font-family:monospace;">${TRANSLATIONS[currentLang].pool_details}</div>
            </div>
        </div>
    `;
    document.getElementById('poolDetailsTitle').innerHTML = hHtml;
    document.getElementById('poolDetailsSub').style.display = 'none';

    // Inject translated tabs
    const tabContainer = document.getElementById('poolDetailsTabs');
    if (tabContainer) {
        tabContainer.innerHTML = `
            <button id="ptab-providers" class="tab-btn" onclick="openPoolTab('providers')">${TRANSLATIONS[currentLang].providers}</button>
            <button id="ptab-activity" class="tab-btn" onclick="openPoolTab('activity')">${TRANSLATIONS[currentLang].activity}</button>
        `;
    }

    document.body.style.overflow = 'hidden';

    openPoolTab(initialTab);
}

function closePoolDetailsModal() {
    document.getElementById('poolDetailsModal').style.display = 'none';
    document.body.style.overflow = '';
}

function openPoolTab(tab) {
    document.querySelectorAll('#poolDetailsModal .tab-btn').forEach(b => b.classList.remove('active'));
    document.getElementById(`ptab-${tab}`).classList.add('active');

    document.querySelectorAll('#poolDetailsModal .wtab-content').forEach(c => c.style.display = 'none');
    document.getElementById(`pview-${tab}`).style.display = 'block';

    if (window.currentPoolBase) {
        if (tab === 'providers') loadPoolProviders(window.currentPoolBase, window.currentPoolTarget);
        if (tab === 'activity') loadPoolActivity(window.currentBaseSym, window.currentTargetSym);
    }
}

async function loadPoolProviders(base, target) {
    const tbody = document.getElementById('poolProvidersTable');
    tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;">${TRANSLATIONS[currentLang].loading}</td></tr>`;

    // Set headers if possible (optional, or rely on static HTML if updated)
    // Actually static HTML in openPoolDetails needs to change, or we inject here

    try {
        const res = await fetch(`/pool/providers?base=${encodeURIComponent(base)}&target=${encodeURIComponent(target)}`);
        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Server Error ${res.status}: ${text.substring(0, 50)}...`);
        }
        const data = await res.json();

        if (!data || data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;">${TRANSLATIONS[currentLang].no_providers_found}</td></tr>`;
            return;
        }

        tbody.innerHTML = '';
        data.forEach((p, i) => {
            // Use formatAddress for correct alias display
            const walletDisplay = formatAddress(p.address);
            tbody.innerHTML += `
                <tr>
                    <td>#${i + 1}</td>
                    <td>
                        <span class="clickable-address" onclick="openWalletDetails('${p.address}')" style="font-family:monospace; color:var(--text-primary); font-weight:bold; cursor:pointer;">
                            ${walletDisplay}
                        </span>
                    </td>
                    <td style="text-align:right;">${p.balance.toLocaleString(undefined, { maximumFractionDigits: 4 })} ${TRANSLATIONS[currentLang].shares}</td>
                </tr>
            `;
        });
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="3" style="text-align:center; color:red;">${e.message}</td></tr>`;
    }
}

async function loadPoolActivity(base, target) {
    const tbody = document.getElementById('poolActivityTable');
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;">Loading...</td></tr>';

    try {
        const res = await fetch(`/pool/activity?base=${encodeURIComponent(base)}&target=${encodeURIComponent(target)}`);
        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Server Error ${res.status}: ${text.substring(0, 50)}...`);
        }
        const data = await res.json();

        if (!data || data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;">No recent activity</td></tr>';
            return;
        }

        tbody.innerHTML = '';
        data.forEach(a => {
            // Format amounts nicely (no division needed)
            const baseAmt = parseFloat(a.base_amount);
            const targetAmt = parseFloat(a.target_amount);
            const usdVal = parseFloat(a.usd_value || 0);

            const baseStr = formatAmount(baseAmt);
            const targetStr = formatAmount(targetAmt);
            const usdStr = usdVal.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

            const isDeposit = a.type === 'deposit';
            const typeColor = isDeposit ? '#10B981' : '#EF4444';
            const typeIcon = isDeposit ? '+' : '-';

            tbody.innerHTML += `
                <tr>
                    <td style="font-size:12px; color:var(--text-secondary);">${a.time}</td>
                    <td>
                         <span class="clickable-address" onclick="openWalletDetails('${a.wallet}')" style="font-family:monospace; color:var(--text-primary); font-weight:bold; cursor:pointer;">
                            ${formatAddress(a.wallet)}
                        </span>
                    </td>
                    <td style="text-align:right;">
                        <div style="color:${typeColor}; font-weight:bold;">$${usdStr}</div>
                        <div style="font-size:11px; color:var(--text-secondary);">${typeIcon}${baseStr} ${window.currentBaseSym}</div>
                        <div style="font-size:11px; color:var(--text-secondary);">${typeIcon}${targetStr} ${window.currentTargetSym}</div>
                    </td>
                </tr>
            `;
        });
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="4" style="text-align:center; color:red;">${e.message}</td></tr>`;
    }
}

// --- TRENDING TOKENS DONUT ---
let trendingChart = null;

async function loadTrendingTokens() {
    try {
        const tfEl = document.getElementById('header-timeframe');
        const tf = tfEl ? tfEl.value : '24h';
        const res = await fetch(`/stats/trending-tokens?timeframe=${tf}`);
        const data = await res.json();

        // Data: [{symbol, volume, logo}, ...]
        if (!data || data.length === 0) {
            const legend = document.getElementById('trending-legend');
            if (legend) legend.innerHTML = '<div style="text-align:center; color:#999;">No data</div>';
            return;
        }

        // Colors for Chart & Legend (High Contrast)
        const colors = ['#D0021B', '#3B82F6', '#10B981', '#F59E0B', '#8B5CF6']; // Red, Blue, Green, Amber, Purple

        // Render Legend
        const legendContainer = document.getElementById('trending-legend');
        if (legendContainer) {
            let legendHtml = '';
            data.forEach((t, i) => {
                const logo = getProxyUrl(t.logo);
                // Use matching color for the underline
                const lineColor = colors[i % colors.length];

                // New Design: Left Border Indicator
                legendHtml += `
                    <div style="width:100%; display:flex; justify-content:space-between; align-items:center; padding:8px 0 8px 10px; border-left:4px solid ${lineColor}; background:rgba(255,255,255,0.02); margin-bottom:4px; border-radius:0 4px 4px 0;">
                        <div style="display:flex; align-items:center; gap:8px;">
                            <img src="${logo}" style="width:20px; height:20px; border-radius:50%;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
                            <span style="font-weight:600; font-size:13px;">${t.symbol}</span>
                        </div>
                        <div style="font-size:13px; color:#6B7280; padding-right:8px;">$${parseFloat(t.volume).toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                    </div>
                `;
            });
            legendContainer.innerHTML = legendHtml;
        }

        // Render Chart
        const ctx = document.getElementById('trendingDonutChart');
        if (ctx) {
            if (trendingChart) trendingChart.destroy();

            const labels = data.map(d => d.symbol);
            const values = data.map(d => d.volume);

            trendingChart = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: labels,
                    datasets: [{
                        data: values,
                        backgroundColor: colors,
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: function (context) {
                                    let label = context.label || '';
                                    if (label) label += ': ';
                                    if (context.parsed !== null) label += '$' + context.parsed.toLocaleString(undefined, { maximumFractionDigits: 0 });
                                    return label;
                                }
                            }
                        }
                    },
                    cutout: '70%'
                }
            });
        }

    } catch (e) {
        console.error("Error loading trending tokens:", e);
    }
}

// --- STABLECOIN MONITOR ---
let pegChart = null;

async function loadStablecoinMonitor() {
    console.log("🚀 Running loadStablecoinMonitor V3 - Horizontal Layout");
    try {
        const tfEl = document.getElementById('header-timeframe');
        const tf = tfEl ? tfEl.value : '24h';
        const res = await fetch(`/stats/stablecoins?timeframe=${tf}`);
        const data = await res.json();

        // 1. Render List
        const listContainer = document.getElementById('stablecoin-list');
        if (listContainer) {
            let html = '';
            data.forEach(t => {
                const isTBCD = t.symbol === 'TBCD';

                // Format price - use scientific notation for very small values
                let priceFmt;
                if (t.price < 0.0001 && t.price > 0) {
                    // Scientific notation: e.g. $0.0...2918
                    // Fix: Ensure we correctly calculate the subscript and don't double dots
                    const str = t.price.toFixed(20); // Get enough precision
                    const match = str.match(/^0\.0+([1-9]\d*)/);
                    if (match) {
                        const decimals = match[0].length - match[1].length - 2; // count zeros after dot
                        const mantissa = match[1].substring(0, 4); // first 4 sig digits
                        // subscript map
                        const subs = ['₀', '₁', '₂', '₃', '₄', '₅', '₆', '₇', '₈', '₉'];
                        const decimalsStr = decimals.toString().split('').map(d => subs[parseInt(d)]).join('');
                        priceFmt = `$0.0${decimalsStr}${mantissa}`;
                    } else {
                        priceFmt = '$' + t.price.toFixed(4);
                    }
                } else {
                    priceFmt = '$' + t.price.toFixed(isTBCD ? 4 : 4);
                }

                // Format Volumes
                const swapVolFmt = '$' + parseFloat(t.swapVolume || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
                const transVolFmt = '$' + parseFloat(t.transferVolume || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });

                // Deviation %
                const devRaw = t.price - 1;
                const devPct = (Math.abs(devRaw) * 100).toFixed(2);
                const devSign = devRaw > 0 ? '+' : (devRaw < 0 ? '-' : '');

                // Color logic
                let priceColor = '#10B981'; // Green
                let devMsg = 'Pegged';
                if (Math.abs(devRaw) > 0.005) { priceColor = '#F59E0B'; devMsg = 'Drifting'; } // 0.5%
                if (Math.abs(devRaw) > 0.02) { priceColor = '#D0021B'; devMsg = 'Depegged'; } // 2%

                // Logo URL - DIRECT use, bypass proxy to fix potential issues
                const logoUrl = t.logo || LOCAL_PLACEHOLDER;

                html += `
                <div style="flex:1; min-width:250px; background:var(--bg-body); padding:15px; border-radius:12px; border:1px solid var(--border-color); display:flex; flex-direction:column; gap:8px;">
                    <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:8px;">
                        <div style="display:flex; align-items:center; gap:8px;">
                            <img src="${logoUrl}" style="width:28px; height:28px; border-radius:50%; background:#fff;" onerror="this.src='${LOCAL_PLACEHOLDER}'">
                            <span style="font-weight:700; font-size:14px;">${t.symbol}</span>
                        </div>
                        <span style="font-family:'Roboto Mono'; font-weight:700; font-size:18px; color:${priceColor};">${priceFmt}</span>
                    </div>
                    
                    <div style="display:flex; flex-direction:column; gap:4px; font-size:12px; color:#9CA3AF;">
                        <div style="display:flex; justify-content:space-between;">
                           <span>Swap Vol:</span>
                           <span style="color:#E5E7EB; font-weight:600;">${swapVolFmt}</span>
                        </div>
                        <div style="display:flex; justify-content:space-between;">
                           <span>Transfer Vol:</span>
                           <span style="color:#E5E7EB; font-weight:600;">${transVolFmt}</span>
                        </div>
                    </div>

                    <div style="margin-top:auto; padding-top:8px; display:flex; justify-content:space-between; align-items:center; font-size:11px;">
                        <span style="color:#6B7280;">Deviation:</span>
                        <span style="font-weight:700; color:${priceColor};">${devSign}${devPct}% (${devMsg})</span>
                    </div>
                </div>`;
            });
            listContainer.innerHTML = html;
        }

        // 2. Render Chart
        const ctx = document.getElementById('pegChart');
        if (ctx) {
            if (pegChart) pegChart.destroy();

            // Prepare datasets with thicker lines
            const colors = { 'KUSD': '#3B82F6', 'XSTUSD': '#D0021B', 'TBCD': '#10B981' };
            const datasets = data.map(t => {
                return {
                    label: t.symbol,
                    data: t.sparkline.map(p => p.value),
                    borderColor: colors[t.symbol] || '#999',
                    borderWidth: 3,
                    pointRadius: 0,
                    tension: 0.2,
                    fill: false
                };
            });

            // Labels
            const labels = data[0]?.sparkline.map(p => {
                const date = new Date(p.time);
                return tf === '24h' ? date.toLocaleTimeString() : date.toLocaleDateString();
            }) || [];

            pegChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: datasets
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        mode: 'index',
                        intersect: false,
                    },
                    plugins: {
                        legend: { display: true, position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } },
                        tooltip: {
                            callbacks: {
                                label: function (context) {
                                    return context.dataset.label + ': $' + context.parsed.y.toFixed(4);
                                }
                            }
                        },
                        annotation: {
                            annotations: {
                                line1: {
                                    type: 'line',
                                    yMin: 1,
                                    yMax: 1,
                                    borderColor: '#666',
                                    borderWidth: 1,
                                    borderDash: [5, 5],
                                    label: {
                                        content: '$1.00 Peg',
                                        enabled: true,
                                        position: 'end',
                                        color: '#999',
                                        font: { size: 10 }
                                    }
                                }
                            }
                        }
                    },
                    scales: {
                        x: { display: false },
                        y: {
                            display: true,
                            position: 'right',
                            grid: { color: 'rgba(255,255,255,0.05)' }
                        }
                    }
                }
            });
        }

    } catch (e) {
        console.error("Error loading stablecoin monitor:", e);
    }
}

// --- GLOBAL LIQUIDITY ACTIVITY ---
let liquidityPage = 1;
let liquidityTotalPages = 1;

async function loadGlobalLiquidity(reset = false) {
    if (reset) liquidityPage = 1;
    const tbody = document.getElementById('liquidityTable');
    if (!tbody) return;

    if (reset) tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;">${TRANSLATIONS[currentLang].loading}</td></tr>`;

    // Date Filter
    let timestampQuery = '';
    const dateInput = document.getElementById('liquidityDateInput');
    if (dateInput && dateInput.value) {
        const date = new Date(dateInput.value);
        timestampQuery = `&timestamp=${date.getTime()}`;
    }

    try {
        const res = await fetch(`/history/global/liquidity?page=${liquidityPage}&limit=10${timestampQuery}`);
        const json = await res.json();
        const data = json.data || [];
        const total = json.total || 0;
        liquidityTotalPages = Math.ceil(total / 10) || 1;

        updatePagination('liquidity', liquidityPage, liquidityTotalPages, total);

        if (data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;">${TRANSLATIONS[currentLang].no_data}</td></tr>`;
            return;
        }

        tbody.innerHTML = '';
        data.forEach(d => {
            const timeDate = new Date(d.timestamp);
            const timeStr = timeDate.toLocaleTimeString() + ' ' + timeDate.toLocaleDateString();
            const walletShort = formatAddress(d.wallet);

            // Type
            const isDeposit = d.type === 'deposit';
            const typeLabel = isDeposit ? 'ADD' : 'REMOVE';
            const typeColor = isDeposit ? '#10B981' : '#EF4444';

            // Amount formatting
            const baseAmt = parseFloat(d.base_amount).toLocaleString(undefined, { maximumFractionDigits: 4 });
            const targetAmt = parseFloat(d.target_amount).toLocaleString(undefined, { maximumFractionDigits: 4 });

            // Logos
            const baseLogo = getProxyUrl(d.base_logo);
            const targetLogo = getProxyUrl(d.target_logo);

            tbody.innerHTML += `
                <tr>
                    <td style="color:#6B7280; font-size:13px;">${timeStr}</td>
                    <td style="font-family:monospace;"><a href="#" onclick="openBlockModal('${d.block}'); return false;" style="color:#D0021B;">#${d.block}</a></td>
                    <td>
                        <div style="display:flex; align-items:center; gap:5px;">
                            <div style="display:flex; position:relative; width:36px;">
                                <img src="${baseLogo}" style="width:20px; height:20px; border-radius:50%; z-index:2; border:1px solid var(--bg-card);" onerror="this.style.display='none'">
                                <img src="${targetLogo}" style="width:20px; height:20px; border-radius:50%; position:absolute; left:12px; z-index:1;" onerror="this.onerror=null;this.src='${LOCAL_PLACEHOLDER}'">
                            </div>
                            <span style="font-weight:600; font-size:13px;">${d.pool_base}-${d.pool_target}</span>
                        </div>
                    </td>
                    <td><span style="font-weight:bold; color:${typeColor}; background:rgba(${isDeposit ? '16,185,129' : '239,68,68'}, 0.1); padding:2px 6px; border-radius:4px; font-size:11px;">${typeLabel}</span></td>
                    <td style="font-size:13px;">
                        <div>${baseAmt} ${d.pool_base}</div>
                        <div style="color:#6B7280; font-size:11px;">${targetAmt} ${d.pool_target}</div>
                        <div style="color:#10B981; font-size:11px; font-weight:bold;">$${parseFloat(d.usd_value).toLocaleString()}</div>
                    </td>
                    <td><span onclick="openWalletDetails('${d.wallet}')" class="clickable-address ${walletShort.includes('...') ? '' : 'wallet-alias'}">${walletShort}</span></td>
                    <td>
                        <button class="btn-ghost" onclick="openTxModal('${d.hash}', '${d.extrinsic_id}')" style="font-size:11px; padding:2px 6px;">🔍</button>
                    </td>
                </tr>
            `;
        });
    } catch (e) {
        console.error(e);
        tbody.innerHTML = `<tr><td colspan="7" style="text-align:center; color:red;">${TRANSLATIONS[currentLang].error_loading}</td></tr>`;
    }
}

function changeLiquidityPage(dir) {
    if (dir === 'first') liquidityPage = 1;
    else if (dir === 'last') liquidityPage = liquidityTotalPages;
    else liquidityPage += dir;

    if (liquidityPage < 1) liquidityPage = 1;
    if (liquidityPage > liquidityTotalPages) liquidityPage = liquidityTotalPages;

    loadGlobalLiquidity();
}

// --- CHARTING LOGIC (Lightweight Charts) ---
let chart;
let candlestickSeries;
let currentChartSymbol = '';

function closeChartModal() {
    document.getElementById('chartModal').style.display = 'none';
    document.body.style.overflow = ''; // Unlock Body Scroll
    if (chart) { chart.remove(); chart = null; }
}

// Make global for inline HTML calls
window.showChart = showChart;

function showChart(symbol, resolution = 60) {
    if (typeof LightweightCharts === 'undefined') {
        alert('Error: La librería de gráficos no se ha cargado. Por favor, comprueba tu conexión a internet o recarga la página.');
        return;
    }
    // DEBUG: Remove after fixing
    // alert('Debug: showChart calling for ' + symbol); 

    window.currentChartSymbol = symbol;
    document.getElementById('chartModal').style.display = 'flex';
    document.body.style.overflow = 'hidden'; // Lock Body Scroll
    document.getElementById('chartTitle').innerText = symbol + " / USD";

    // Prevent background touchmove
    document.getElementById('chartModal').ontouchmove = (e) => {
        if (e.target.id !== 'chartContainer' && !document.getElementById('chartContainer').contains(e.target)) {
            e.preventDefault();
        }
    };

    // Update buttons state
    document.querySelectorAll('.time-btn').forEach(b => {
        b.classList.remove('active');
        // Extract number from onclick="updateChartTimeframe(60)"
        const match = b.getAttribute('onclick').match(/\d+/);
        if (match && parseInt(match[0]) === resolution) b.classList.add('active');
    });

    // Clear previous chart
    document.getElementById('chartContainer').innerHTML = '';

    // Create chart with precision config (Wait for DOM layout)
    requestAnimationFrame(() => {
        const container = document.getElementById('chartContainer');
        const width = container.getBoundingClientRect().width;
        const height = container.clientHeight;

        const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
        const bgColor = isDark ? '#1a1a1a' : '#ffffff';
        const textColor = isDark ? '#d1d5db' : '#333';
        const gridColor = isDark ? '#333' : '#f0f0f0';

        chart = LightweightCharts.createChart(container, {
            width: width,
            height: height,
            layout: { backgroundColor: bgColor, textColor: textColor, attributionLogo: false, fontSize: 11 },
            grid: { vertLines: { color: gridColor }, horzLines: { color: gridColor } },
            timeScale: {
                timeVisible: true,
                secondsVisible: false,
                rightOffset: 12,
                barSpacing: 10,
                fixLeftEdge: false,
                lockVisibleTimeRangeOnResize: true,
                visible: true,
                borderColor: isDark ? '#4b5563' : '#D1D5DB',
            },
            rightPriceScale: {
                scaleMargins: { top: 0.1, bottom: 0.25 },
                visible: true,
                borderVisible: true,
                borderColor: isDark ? '#4b5563' : '#D1D5DB',
            },
            handleScale: {
                axisPressedMouseMove: true,
                pinch: true,
                mouseWheel: true,
            },
            handleScroll: {
                mouseWheel: true,
                pressedMouseMove: true,
                horzTouchDrag: true,
                vertTouchDrag: false,
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
        });

        candlestickSeries = chart.addCandlestickSeries({
            upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350',
            priceFormat: {
                type: 'price',
                precision: 10,
                minMove: 0.0000000001,
            },
        });

        fetch(`/chart/${symbol}?res=${resolution}`)
            .then(r => r.json())
            .then(data => {
                if (data.length > 0) {
                    lastChartData = data; // Store for indicators
                    candlestickSeries.setData(data);
                    chart.timeScale().fitContent();
                }
            })
            .catch(err => console.error(err));

        new ResizeObserver(entries => {
            if (entries.length === 0 || entries[0].target !== document.getElementById('chartContainer')) { return; }
            const newRect = entries[0].contentRect;
            chart.applyOptions({ width: newRect.width, height: newRect.height });
        }).observe(document.getElementById('chartContainer'));
    });
}

function updateChartTimeframe(res) {
    if (currentChartSymbol) showChart(currentChartSymbol, res);
}

// --- CHART UTILITIES (Fullscreen & Indicators) ---
let smaSeries = null;
let emaSeries = null;
let lastChartData = [];

function toggleChartFullscreen() {
    const modal = document.getElementById('chartModal');
    const container = document.getElementById('chartContainer');
    const btn = document.querySelector('button[onclick="toggleChartFullscreen()"]');

    // Toggle Class
    const isFull = modal.classList.toggle('fullscreen-chart');

    // Update Styles
    if (isFull) {
        modal.style.width = '100vw';
        modal.style.height = '100vh';
        modal.style.maxWidth = '100%';
        modal.style.borderRadius = '0';
        modal.style.top = '0';
        modal.style.left = '0';
        modal.querySelector('.modal-content').style.width = '100%';
        modal.querySelector('.modal-content').style.height = '100%';
        modal.querySelector('.modal-content').style.maxWidth = '100%';
        modal.querySelector('.modal-content').style.maxHeight = '100%';
        modal.querySelector('.modal-content').style.borderRadius = '0';
        container.style.height = 'calc(100vh - 60px)'; // Adjust header
        btn.innerText = '🗗'; // Restore icon
    } else {
        modal.style.width = '';
        modal.style.height = '';
        modal.style.maxWidth = '';
        modal.style.borderRadius = '';
        modal.style.top = '';
        modal.style.left = '';
        modal.querySelector('.modal-content').style.width = '800px';
        modal.querySelector('.modal-content').style.height = '';
        modal.querySelector('.modal-content').style.maxWidth = '95%';
        modal.querySelector('.modal-content').style.borderRadius = '';
        container.style.height = '400px';
        btn.innerText = '⛶'; // Fullscreen icon
    }

    // Trigger resize
    if (chart) {
        chart.applyOptions({
            width: container.getBoundingClientRect().width,
            height: container.clientHeight
        });
        chart.timeScale().fitContent();
    }
}

function toggleIndicatorsMenu() {
    const menu = document.getElementById('indicatorsMenu');
    menu.style.display = (menu.style.display === 'none') ? 'block' : 'none';
}

// Close menu when clicking outside
window.addEventListener('click', (e) => {
    const menu = document.getElementById('indicatorsMenu');
    if (menu && !e.target.closest('.dropdown')) {
        menu.style.display = 'none';
    }
});

function toggleIndicator(type) {
    if (!chart || lastChartData.length === 0) return;

    if (type === 'SMA') {
        const chk = document.getElementById('chkSMA');
        if (chk.checked) {
            if (!smaSeries) {
                smaSeries = chart.addLineSeries({ color: '#2962FF', lineWidth: 2, title: 'SMA 20' });
            }
            const smaData = calculateSMA(lastChartData, 20);
            smaSeries.setData(smaData);
        } else {
            if (smaSeries) {
                chart.removeSeries(smaSeries);
                smaSeries = null;
            }
        }
    }

    if (type === 'EMA') {
        const chk = document.getElementById('chkEMA');
        if (chk.checked) {
            if (!emaSeries) {
                emaSeries = chart.addLineSeries({ color: '#E91E63', lineWidth: 2, title: 'EMA 20' });
            }
            const emaData = calculateEMA(lastChartData, 20);
            emaSeries.setData(emaData);
        } else {
            if (emaSeries) {
                chart.removeSeries(emaSeries);
                emaSeries = null;
            }
        }
    }
}

function calculateSMA(data, period) {
    const result = [];
    for (let i = 0; i < data.length; i++) {
        if (i < period - 1) continue;
        let sum = 0;
        for (let j = 0; j < period; j++) {
            sum += data[i - j].close; // Lightweight charts use 'close' or 'value'
        }
        result.push({ time: data[i].time, value: sum / period });
    }
    return result;
}

function calculateEMA(data, period) {
    const result = [];
    const k = 2 / (period + 1);
    let ema = data[0].close;

    for (let i = 0; i < data.length; i++) {
        if (i === 0) {
            // First SMA as seed
            // Skip precise seed logic for simpilcity or implement if strict
            ema = data[i].close;
        } else {
            ema = (data[i].close * k) + (ema * (1 - k));
        }

        if (i >= period - 1) { // Only show after period
            result.push({ time: data[i].time, value: ema });
        }
    }
    return result;
}

// --- GLOBAL EVENT DELEGATION FOR DYNAMIC ELEMENTS ---
document.addEventListener('click', function (e) {
    const chartBtn = e.target.closest('.js-show-chart');
    if (chartBtn) {
        e.preventDefault();
        e.stopPropagation();
        const symbol = chartBtn.getAttribute('data-symbol');
        if (symbol) showChart(symbol);
    }
});

document.addEventListener('touchend', function (e) {
    const chartBtn = e.target.closest('.js-show-chart');
    if (chartBtn) {
        e.preventDefault();
        const symbol = chartBtn.getAttribute('data-symbol');
        if (symbol) showChart(symbol);
    }
}, { passive: false });