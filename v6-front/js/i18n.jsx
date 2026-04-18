/* global React */
/* =========================================================================
   i18n — 14 languages, RTL-aware, context provider + useT hook + LangPicker

   NOTE: Governance sub-tab labels (Consejo / Elecciones / Mociones /
   Democracia / Comité Técnico) are intentionally kept in Spanish across
   ALL locales — they are proper names of the SORA on-chain governance
   structure, not translatable UI chrome.
   ========================================================================= */
const { createContext, useContext, useState, useEffect, useMemo, useRef, useCallback } = React;

const LANGS = [
  { code: 'es', native: 'Español',     flag: '🇪🇸', rtl: false },
  { code: 'en', native: 'English',     flag: '🇬🇧', rtl: false },
  { code: 'fr', native: 'Français',    flag: '🇫🇷', rtl: false },
  { code: 'de', native: 'Deutsch',     flag: '🇩🇪', rtl: false },
  { code: 'it', native: 'Italiano',    flag: '🇮🇹', rtl: false },
  { code: 'pt', native: 'Português',   flag: '🇵🇹', rtl: false },
  { code: 'ru', native: 'Русский',     flag: '🇷🇺', rtl: false },
  { code: 'zh', native: '中文',         flag: '🇨🇳', rtl: false },
  { code: 'ja', native: '日本語',       flag: '🇯🇵', rtl: false },
  { code: 'ko', native: '한국어',        flag: '🇰🇷', rtl: false },
  { code: 'ar', native: 'العربية',     flag: '🇸🇦', rtl: true  },
  { code: 'he', native: 'עברית',       flag: '🇮🇱', rtl: true  },
  { code: 'ur', native: 'اردو',        flag: '🇵🇰', rtl: true  },
  { code: 'hi', native: 'हिन्दी',        flag: '🇮🇳', rtl: false },
];

const LANG_BY_CODE = Object.fromEntries(LANGS.map(l => [l.code, l]));

/* ---------- Translation dictionary ---------------------------------------
   Structure: DICT[key][lang] = string
   Key convention: dot-separated. Missing → English → key itself.
--------------------------------------------------------------------------- */
const DICT = {
  /* ===== Nav groups ===== */
  'nav.featured':    { en:'Featured',   es:'Destacados', fr:'En vedette', de:'Empfohlen', it:'In evidenza', pt:'Destaques',  ru:'Избранное', zh:'精选',    ja:'注目',     ko:'추천',       ar:'مميز',        he:'מומלצים',    ur:'نمایاں',      hi:'विशेष' },
  'nav.network':     { en:'Network',    es:'Red',        fr:'Réseau',     de:'Netzwerk',  it:'Rete',        pt:'Rede',       ru:'Сеть',      zh:'网络',    ja:'ネットワーク', ko:'네트워크',     ar:'الشبكة',      he:'רשת',        ur:'نیٹ ورک',     hi:'नेटवर्क' },
  'nav.my':          { en:'My',         es:'Mi',         fr:'Mon',        de:'Mein',      it:'Mio',         pt:'Meu',        ru:'Моё',       zh:'我的',    ja:'マイ',      ko:'내',         ar:'خاصتي',       he:'שלי',        ur:'میرا',        hi:'मेरा' },

  /* ===== Nav items ===== */
  'nav.burnTracker': { en:'Burn Tracker',   es:'Rastreador de Quemas', fr:'Suivi des burns',     de:'Burn-Tracker',      it:'Tracker di burn',      pt:'Rastreador de queimas', ru:'Трекер сжиганий',    zh:'销毁追踪',  ja:'バーントラッカー', ko:'소각 추적',    ar:'متتبع الحرق',      he:'מעקב שריפה',  ur:'برن ٹریکر',       hi:'बर्न ट्रैकर' },
  'nav.pulse':       { en:'Network Pulse',  es:'Pulso de la Red',      fr:'Pouls du réseau',     de:'Netzwerk-Puls',     it:'Pulso di rete',        pt:'Pulso da rede',         ru:'Пульс сети',         zh:'网络脉动',  ja:'ネットワーク鼓動', ko:'네트워크 맥박', ar:'نبض الشبكة',       he:'דופק הרשת',   ur:'نیٹ ورک پلس',      hi:'नेटवर्क पल्स' },
  'nav.intelligence':{ en:'Intelligence',   es:'Inteligencia',         fr:'Intelligence',        de:'Analyse',           it:'Intelligence',         pt:'Inteligência',          ru:'Аналитика',          zh:'情报',      ja:'インテリジェンス', ko:'인텔리전스',   ar:'استخبارات',        he:'מודיעין',    ur:'انٹیلیجنس',        hi:'इंटेलिजेंस' },
  'nav.swaps':       { en:'Swaps',          es:'Intercambios',         fr:'Swaps',               de:'Tausch',            it:'Swap',                 pt:'Swaps',                 ru:'Обмены',             zh:'兑换',      ja:'スワップ',      ko:'스왑',         ar:'المبادلات',        he:'החלפות',     ur:'سواپس',           hi:'स्वैप' },
  'nav.extrinsics':  { en:'Extrinsics',     es:'Extrínsecos',          fr:'Extrinsèques',        de:'Extrinsics',        it:'Extrinsic',            pt:'Extrínsecos',           ru:'Экстринсики',        zh:'外部交易',  ja:'エクストリンシック', ko:'익스트린식',    ar:'المعاملات',        he:'אקסטרינסיקים', ur:'ایکسٹرنسکس',       hi:'एक्सट्रिंसिक्स' },
  'nav.transfers':   { en:'Transfers',      es:'Transferencias',       fr:'Transferts',          de:'Überweisungen',     it:'Trasferimenti',        pt:'Transferências',        ru:'Переводы',           zh:'转账',      ja:'送金',          ko:'전송',         ar:'التحويلات',        he:'העברות',     ur:'منتقلیاں',         hi:'स्थानांतरण' },
  'nav.bridges':     { en:'Bridges',        es:'Puentes',              fr:'Ponts',               de:'Brücken',           it:'Ponti',                pt:'Pontes',                ru:'Мосты',              zh:'跨链桥',    ja:'ブリッジ',      ko:'브릿지',       ar:'الجسور',           he:'גשרים',      ur:'پل',               hi:'ब्रिज' },
  'nav.orderBook':   { en:'Order Book',     es:'Libro de Órdenes',     fr:'Carnet d\'ordres',    de:'Orderbuch',         it:'Libro ordini',         pt:'Livro de ordens',       ru:'Стакан заявок',      zh:'订单簿',    ja:'オーダーブック',  ko:'오더북',       ar:'دفتر الأوامر',     he:'ספר הזמנות',  ur:'آرڈر بک',         hi:'ऑर्डर बुक' },
  'nav.pools':       { en:'Pools',          es:'Pools',                fr:'Pools',               de:'Pools',             it:'Pool',                 pt:'Pools',                 ru:'Пулы',               zh:'流动性池',  ja:'プール',        ko:'풀',           ar:'المجمعات',         he:'בריכות',     ur:'پولز',            hi:'पूल' },
  'nav.tokens':      { en:'Tokens',         es:'Tokens',               fr:'Jetons',              de:'Tokens',            it:'Token',                pt:'Tokens',                ru:'Токены',             zh:'代币',      ja:'トークン',      ko:'토큰',         ar:'العملات',          he:'אסימונים',   ur:'ٹوکنز',           hi:'टोकन' },
  'nav.holders':     { en:'Holders',        es:'Titulares',            fr:'Détenteurs',          de:'Halter',            it:'Detentori',            pt:'Detentores',            ru:'Держатели',          zh:'持有者',    ja:'保有者',        ko:'보유자',       ar:'الحائزون',         he:'מחזיקים',    ur:'ہولڈرز',           hi:'धारक' },
  'nav.staking':     { en:'Staking',        es:'Staking',              fr:'Staking',             de:'Staking',           it:'Staking',              pt:'Staking',               ru:'Стейкинг',           zh:'质押',      ja:'ステーキング',  ko:'스테이킹',     ar:'الرهن',            he:'נעילה',      ur:'اسٹیکنگ',          hi:'स्टेकिंग' },
  'nav.governance':  { en:'Governance',     es:'Gobernanza',           fr:'Gouvernance',         de:'Governance',        it:'Governance',           pt:'Governança',            ru:'Управление',         zh:'治理',      ja:'ガバナンス',    ko:'거버넌스',     ar:'الحوكمة',          he:'ממשל',       ur:'گورننس',          hi:'शासन' },
  'nav.portfolio':   { en:'Portfolio',      es:'Cartera',              fr:'Portefeuille',        de:'Portfolio',         it:'Portafoglio',          pt:'Carteira',              ru:'Портфель',           zh:'投资组合',  ja:'ポートフォリオ', ko:'포트폴리오',   ar:'المحفظة',          he:'תיק',        ur:'پورٹ فولیو',      hi:'पोर्टफ़ोलियो' },
  'nav.balance':     { en:'Balance',        es:'Saldo',                fr:'Solde',               de:'Guthaben',          it:'Saldo',                pt:'Saldo',                 ru:'Баланс',             zh:'余额',      ja:'残高',          ko:'잔액',         ar:'الرصيد',           he:'יתרה',       ur:'بیلنس',           hi:'शेष' },

  /* ===== Common ===== */
  'common.search':     { en:'Search wallet, tx hash, block, extrinsic…',  es:'Buscar cartera, hash, bloque, extrínseco…', fr:'Rechercher wallet, hash, bloc, extrinsèque…', de:'Wallet, Hash, Block, Extrinsic suchen…',  it:'Cerca wallet, hash, blocco, extrinsic…',  pt:'Buscar carteira, hash, bloco, extrínseco…',  ru:'Поиск кошелька, хэша, блока…',  zh:'搜索钱包、哈希、区块、外部交易…',  ja:'ウォレット・ハッシュ・ブロック…',  ko:'지갑, 해시, 블록, 익스트린식 검색…',  ar:'ابحث في المحافظ والكتل والمعاملات…',  he:'חיפוש ארנק, האש, בלוק…',  ur:'والیٹ، ہیش، بلاک تلاش کریں…',  hi:'वॉलेट, हैश, ब्लॉक खोजें…' },
  'common.live':       { en:'LIVE',         es:'EN VIVO',      fr:'EN DIRECT',    de:'LIVE',      it:'IN DIRETTA',  pt:'AO VIVO',    ru:'В ЭФИРЕ',      zh:'实时',     ja:'ライブ',      ko:'실시간',       ar:'مباشر',       he:'חי',          ur:'براہ راست',    hi:'लाइव' },
  'common.connected':  { en:'connected',    es:'conectado',    fr:'connecté',     de:'verbunden', it:'connesso',    pt:'conectado',  ru:'подключено',   zh:'已连接',   ja:'接続済み',    ko:'연결됨',       ar:'متصل',        he:'מחובר',       ur:'منسلک',        hi:'जुड़ा हुआ' },
  'common.pause':      { en:'Pause',        es:'Pausar',       fr:'Pause',        de:'Pause',     it:'Pausa',       pt:'Pausar',     ru:'Пауза',        zh:'暂停',     ja:'一時停止',    ko:'일시정지',     ar:'إيقاف',       he:'השהה',        ur:'روکیں',        hi:'रोकें' },
  'common.resume':     { en:'Resume',       es:'Reanudar',     fr:'Reprendre',    de:'Fortsetzen',it:'Riprendi',    pt:'Retomar',    ru:'Продолжить',   zh:'继续',     ja:'再開',        ko:'재개',         ar:'استئناف',     he:'המשך',        ur:'جاری رکھیں',   hi:'जारी रखें' },
  'common.copy':       { en:'Copy',         es:'Copiar',       fr:'Copier',       de:'Kopieren',  it:'Copia',       pt:'Copiar',     ru:'Копировать',   zh:'复制',     ja:'コピー',      ko:'복사',         ar:'نسخ',         he:'העתק',        ur:'کاپی',         hi:'कॉपी' },
  'common.copied':     { en:'Copied',       es:'Copiado',      fr:'Copié',        de:'Kopiert',   it:'Copiato',     pt:'Copiado',    ru:'Скопировано',  zh:'已复制',   ja:'コピー済み',  ko:'복사됨',       ar:'تم النسخ',    he:'הועתק',       ur:'کاپی ہو گیا',  hi:'कॉपी किया' },
  'common.close':      { en:'Close',        es:'Cerrar',       fr:'Fermer',       de:'Schließen', it:'Chiudi',      pt:'Fechar',     ru:'Закрыть',      zh:'关闭',     ja:'閉じる',      ko:'닫기',         ar:'إغلاق',       he:'סגור',        ur:'بند کریں',     hi:'बंद' },
  'common.language':   { en:'Language',     es:'Idioma',       fr:'Langue',       de:'Sprache',   it:'Lingua',      pt:'Idioma',     ru:'Язык',         zh:'语言',     ja:'言語',        ko:'언어',         ar:'اللغة',       he:'שפה',         ur:'زبان',         hi:'भाषा' },

  /* ===== Top bar ===== */
  'topbar.block':    { en:'Block',        es:'Bloque',       fr:'Bloc',         de:'Block',     it:'Blocco',      pt:'Bloco',      ru:'Блок',         zh:'区块',     ja:'ブロック',    ko:'블록',         ar:'كتلة',        he:'בלוק',        ur:'بلاک',         hi:'ब्लॉक' },
  'topbar.eraEpoch': { en:'Era · Epoch',  es:'Era · Época',  fr:'Ère · Époque', de:'Ära · Epoche', it:'Era · Epoca', pt:'Era · Época', ru:'Эра · Эпоха', zh:'纪元·周期', ja:'エラ・エポック', ko:'시대·에포크', ar:'حقبة · عصر', he:'עידן · אפוקה', ur:'دور · عہد',   hi:'युग · एपॉक' },

  /* ===== Burn Tracker ===== */
  'burn.title':    { en:'Burn Tracker',                                      es:'Rastreador de Quemas',                                fr:'Suivi des burns',                                   de:'Burn-Tracker',                                    it:'Tracker di burn',                                 pt:'Rastreador de queimas',                             ru:'Трекер сжиганий',                                  zh:'销毁追踪',                                ja:'バーントラッカー',                             ko:'소각 추적기',                             ar:'متتبع الحرق',                                       he:'מעקב שריפה',                                  ur:'برن ٹریکر',                                 hi:'बर्न ट्रैकर' },
  'burn.sub':      { en:'Real-time burn economics across all SORA assets.', es:'Economía de quemas en tiempo real para todos los activos SORA.', fr:'Économie des burns en temps réel sur tous les actifs SORA.', de:'Burn-Ökonomie in Echtzeit für alle SORA-Assets.', it:'Economia dei burn in tempo reale per tutti gli asset SORA.', pt:'Economia de queimas em tempo real para todos os ativos SORA.', ru:'Экономика сжиганий всех активов SORA в реальном времени.', zh:'实时追踪所有 SORA 资产的销毁经济。', ja:'すべての SORA 資産のバーン経済をリアルタイム表示。', ko:'모든 SORA 자산의 실시간 소각 경제.',    ar:'اقتصاديات الحرق الفورية لجميع أصول SORA.',          he:'כלכלת שריפה בזמן אמת לכל נכסי SORA.',           ur:'تمام SORA اثاثوں کی حقیقی وقت میں برن اکنامکس۔', hi:'सभी SORA परिसंपत्तियों की रीयल-टाइम बर्न इकोनॉमिक्स।' },

  /* ===== Pulse ===== */
  'pulse.title':   { en:'Network Pulse',                                     es:'Pulso de la Red',                                     fr:'Pouls du réseau',                                   de:'Netzwerk-Puls',                                   it:'Pulso di rete',                                   pt:'Pulso da rede',                                     ru:'Пульс сети',                                       zh:'网络脉动',                                ja:'ネットワーク鼓動',                            ko:'네트워크 맥박',                           ar:'نبض الشبكة',                                        he:'דופק הרשת',                                   ur:'نیٹ ورک پلس',                              hi:'नेटवर्क पल्स' },
  'pulse.sub':     { en:'Live on-chain activity across blocks, swaps, and transfers.', es:'Actividad en cadena en vivo: bloques, swaps y transferencias.', fr:'Activité en chaîne en direct : blocs, swaps, transferts.', de:'On-Chain-Aktivität: Blöcke, Swaps, Transfers.', it:'Attività on-chain in diretta: blocchi, swap, trasferimenti.', pt:'Atividade on-chain em tempo real: blocos, swaps, transferências.', ru:'Ончейн-активность: блоки, свопы, переводы.', zh:'实时链上活动:区块、兑换、转账。', ja:'ライブオンチェーン活動:ブロック・スワップ・送金。', ko:'실시간 온체인 활동: 블록, 스왑, 전송.', ar:'النشاط الحي على السلسلة: الكتل والمبادلات والتحويلات.', he:'פעילות בזמן אמת בשרשרת: בלוקים, החלפות, העברות.', ur:'براہ راست آن چین سرگرمی: بلاکس، سواپس، منتقلیاں۔', hi:'लाइव ऑन-चेन गतिविधि: ब्लॉक, स्वैप, ट्रांसफ़र।' },

  /* ===== Intelligence ===== */
  'intel.title':   { en:'Intelligence',    es:'Inteligencia',   fr:'Intelligence',   de:'Analyse',       it:'Intelligence',  pt:'Inteligência',   ru:'Аналитика',    zh:'情报',     ja:'インテリジェンス', ko:'인텔리전스',  ar:'استخبارات',   he:'מודיעין',    ur:'انٹیلیجنس',   hi:'इंटेलिजेंस' },
  'intel.sub':     { en:'Curated insights from on-chain pattern analysis.', es:'Perspectivas curadas del análisis de patrones on-chain.', fr:'Analyses tirées de l\'étude des schémas on-chain.', de:'Kuratierte Erkenntnisse aus der On-Chain-Musteranalyse.', it:'Approfondimenti dall\'analisi dei pattern on-chain.', pt:'Insights curados da análise de padrões on-chain.', ru:'Аналитика на основе ончейн-паттернов.', zh:'基于链上模式分析的精选洞察。', ja:'オンチェーンパターン分析からの洞察。', ko:'온체인 패턴 분석 기반 인사이트.', ar:'رؤى مختارة من تحليل الأنماط على السلسلة.', he:'תובנות נבחרות מניתוח דפוסים בשרשרת.', ur:'آن چین پیٹرن تجزیے سے منتخب بصیرت۔', hi:'ऑन-चेन पैटर्न विश्लेषण से चुनी गई अंतर्दृष्टियाँ।' },

  /* ===== Swaps ===== */
  'swaps.title':   { en:'Swaps',   es:'Intercambios',  fr:'Swaps',   de:'Tausch',   it:'Swap',   pt:'Swaps',   ru:'Обмены',   zh:'兑换',   ja:'スワップ',   ko:'스왑',   ar:'المبادلات',   he:'החלפות',  ur:'سواپس',   hi:'स्वैप' },
  'swaps.sub':     { en:'DEX trades routed via liquidity proxy.', es:'Operaciones DEX vía liquidity proxy.', fr:'Trades DEX routés via liquidity proxy.', de:'DEX-Trades via Liquidity-Proxy.', it:'Trade DEX via liquidity proxy.', pt:'Trades DEX via liquidity proxy.', ru:'DEX-сделки через liquidity proxy.', zh:'通过流动性代理路由的 DEX 交易。', ja:'リクイディティプロキシ経由の DEX 取引。', ko:'유동성 프록시 DEX 거래.', ar:'صفقات DEX عبر بروكسي السيولة.', he:'עסקאות DEX דרך ליקווידיטי פרוקסי.', ur:'لکویڈٹی پراکسی کے ذریعے DEX ٹریڈز۔', hi:'लिक्विडिटी प्रॉक्सी के माध्यम से DEX ट्रेड।' },

  /* ===== Extrinsics ===== */
  'extrinsics.title': { en:'Extrinsics',  es:'Extrínsecos',  fr:'Extrinsèques',  de:'Extrinsics',  it:'Extrinsic',  pt:'Extrínsecos',  ru:'Экстринсики',  zh:'外部交易',  ja:'エクストリンシック',  ko:'익스트린식',  ar:'المعاملات',  he:'אקסטרינסיקים',  ur:'ایکسٹرنسکس',  hi:'एक्सट्रिंसिक्स' },
  'extrinsics.sub':   { en:'Every pallet call, signed and traced.', es:'Cada llamada a pallet, firmada y rastreada.', fr:'Chaque appel de pallet, signé et tracé.', de:'Jeder Pallet-Call, signiert und verfolgt.', it:'Ogni chiamata a pallet, firmata e tracciata.', pt:'Cada chamada de pallet, assinada e rastreada.', ru:'Каждый вызов pallet, подписан и отслежен.', zh:'每次模块调用,已签名并追踪。', ja:'すべてのパレット呼び出し、署名済み。', ko:'모든 팔레트 호출, 서명 및 추적.', ar:'كل استدعاء pallet، موقّع ومتتبع.', he:'כל קריאת pallet, חתומה ומתועדת.', ur:'ہر پیلٹ کال، دستخط شدہ اور ٹریس۔', hi:'हर पैलेट कॉल, हस्ताक्षरित और ट्रेस।' },

  /* ===== Transfers ===== */
  'transfers.title': { en:'Transfers',  es:'Transferencias',  fr:'Transferts',  de:'Überweisungen',  it:'Trasferimenti',  pt:'Transferências',  ru:'Переводы',  zh:'转账',  ja:'送金',  ko:'전송',  ar:'التحويلات',  he:'העברות',  ur:'منتقلیاں',  hi:'स्थानांतरण' },
  'transfers.sub':   { en:'Direct balance movements, excluding DEX.', es:'Movimientos directos de saldo, excluyendo DEX.', fr:'Mouvements de solde directs, hors DEX.', de:'Direkte Saldo-Bewegungen, ohne DEX.', it:'Movimenti diretti di saldo, escluso DEX.', pt:'Movimentos diretos de saldo, excluindo DEX.', ru:'Прямые переводы, без DEX.', zh:'直接余额变动(不含 DEX)。', ja:'直接的な残高移動、DEX を除く。', ko:'DEX 제외 직접 잔액 이동.', ar:'تحركات رصيد مباشرة، باستثناء DEX.', he:'תנועות יתרה ישירות, ללא DEX.', ur:'براہ راست بیلنس منتقلی، DEX کے بغیر۔', hi:'प्रत्यक्ष बैलेंस मूवमेंट, DEX को छोड़कर।' },

  /* ===== Bridges ===== */
  'bridges.title':   { en:'Bridges',    es:'Puentes',    fr:'Ponts',    de:'Brücken',    it:'Ponti',    pt:'Pontes',    ru:'Мосты',    zh:'跨链桥',  ja:'ブリッジ',   ko:'브릿지',    ar:'الجسور',    he:'גשרים',    ur:'پل',    hi:'ब्रिज' },
  'bridges.sub':     { en:'Cross-chain transfers via HASHI.', es:'Transferencias entre cadenas vía HASHI.', fr:'Transferts cross-chain via HASHI.', de:'Cross-Chain-Transfers via HASHI.', it:'Trasferimenti cross-chain via HASHI.', pt:'Transferências cross-chain via HASHI.', ru:'Кросс-чейн переводы через HASHI.', zh:'通过 HASHI 的跨链转账。', ja:'HASHI 経由のクロスチェーン送金。', ko:'HASHI 통한 크로스체인 전송.', ar:'تحويلات عبر السلاسل بواسطة HASHI.', he:'העברות חוצות-שרשרת דרך HASHI.', ur:'HASHI کے ذریعے کراس چین منتقلی۔', hi:'HASHI के माध्यम से क्रॉस-चेन ट्रांसफ़र।' },

  /* ===== Order Book ===== */
  'orderbook.title': { en:'Order Book',  es:'Libro de Órdenes',  fr:'Carnet d\'ordres',  de:'Orderbuch',  it:'Libro ordini',  pt:'Livro de ordens',  ru:'Стакан заявок',  zh:'订单簿',  ja:'オーダーブック',  ko:'오더북',  ar:'دفتر الأوامر',  he:'ספר הזמנות',  ur:'آرڈر بک',  hi:'ऑर्डर बुक' },
  'orderbook.sub':   { en:'Native limit-order market depth.', es:'Profundidad del mercado de órdenes limitadas.', fr:'Profondeur du carnet d\'ordres limites.', de:'Markt-Tiefe für Limit-Orders.', it:'Profondità del mercato a ordini limite.', pt:'Profundidade do mercado de ordens limitadas.', ru:'Глубина рынка лимитных ордеров.', zh:'原生限价订单市场深度。', ja:'ネイティブ指値注文の市場深度。', ko:'네이티브 지정가 주문 시장 깊이.', ar:'عمق سوق أوامر الحد.', he:'עומק שוק הזמנות מגבילות.', ur:'لمٹ آرڈر مارکیٹ کی گہرائی۔', hi:'लिमिट-ऑर्डर बाज़ार गहराई।' },

  /* ===== Pools ===== */
  'pools.title':     { en:'Pools',   es:'Pools',   fr:'Pools',   de:'Pools',   it:'Pool',   pt:'Pools',   ru:'Пулы',   zh:'流动性池',  ja:'プール',   ko:'풀',   ar:'المجمعات',   he:'בריכות',   ur:'پولز',   hi:'पूल' },
  'pools.sub':       { en:'Liquidity pools ranked by TVL.', es:'Pools de liquidez ordenados por TVL.', fr:'Pools de liquidité classés par TVL.', de:'Liquiditätspools nach TVL.', it:'Pool di liquidità classificati per TVL.', pt:'Pools de liquidez classificados por TVL.', ru:'Пулы ликвидности по TVL.', zh:'按 TVL 排序的流动性池。', ja:'TVL でランク付けされた流動性プール。', ko:'TVL로 정렬된 유동성 풀.', ar:'مجمعات السيولة حسب TVL.', he:'בריכות נזילות לפי TVL.', ur:'TVL کے لحاظ سے لکویڈٹی پولز۔', hi:'TVL के अनुसार लिक्विडिटी पूल।' },

  /* ===== Tokens ===== */
  'tokens.title':    { en:'Tokens',   es:'Tokens',   fr:'Jetons',   de:'Tokens',   it:'Token',   pt:'Tokens',   ru:'Токены',   zh:'代币',   ja:'トークン',   ko:'토큰',   ar:'العملات',   he:'אסימונים',   ur:'ٹوکنز',   hi:'टोकन' },
  'tokens.sub':      { en:'All registered SORA assets.', es:'Todos los activos SORA registrados.', fr:'Tous les actifs SORA enregistrés.', de:'Alle registrierten SORA-Assets.', it:'Tutti gli asset SORA registrati.', pt:'Todos os ativos SORA registrados.', ru:'Все зарегистрированные активы SORA.', zh:'所有已注册的 SORA 资产。', ja:'登録済みの SORA 資産すべて。', ko:'등록된 모든 SORA 자산.', ar:'جميع أصول SORA المسجّلة.', he:'כל נכסי SORA הרשומים.', ur:'تمام رجسٹرڈ SORA اثاثے۔', hi:'सभी पंजीकृत SORA संपत्तियाँ।' },

  /* ===== Holders ===== */
  'holders.title':   { en:'Holders',   es:'Titulares',   fr:'Détenteurs',   de:'Halter',   it:'Detentori',   pt:'Detentores',   ru:'Держатели',   zh:'持有者',   ja:'保有者',   ko:'보유자',   ar:'الحائزون',   he:'מחזיקים',   ur:'ہولڈرز',   hi:'धारक' },
  'holders.sub':     { en:'Top addresses by XOR balance.', es:'Principales direcciones por saldo XOR.', fr:'Principales adresses par solde XOR.', de:'Top-Adressen nach XOR-Saldo.', it:'Principali indirizzi per saldo XOR.', pt:'Principais endereços por saldo XOR.', ru:'Топ-адреса по балансу XOR.', zh:'按 XOR 余额排名的地址。', ja:'XOR 残高順のトップアドレス。', ko:'XOR 잔액 상위 주소.', ar:'أعلى العناوين برصيد XOR.', he:'כתובות מובילות לפי יתרת XOR.', ur:'XOR بیلنس کے لحاظ سے ٹاپ ایڈریسز۔', hi:'XOR बैलेंस के अनुसार शीर्ष पते।' },

  /* ===== Staking ===== */
  'staking.title':   { en:'Staking',  es:'Staking',  fr:'Staking',  de:'Staking',  it:'Staking',  pt:'Staking',  ru:'Стейкинг',  zh:'质押',  ja:'ステーキング',  ko:'스테이킹',  ar:'الرهن',  he:'נעילה',  ur:'اسٹیکنگ',  hi:'स्टेकिंग' },
  'staking.sub':     { en:'Validators and network staking metrics.', es:'Validadores y métricas de staking.', fr:'Validateurs et métriques de staking.', de:'Validatoren und Staking-Metriken.', it:'Validatori e metriche di staking.', pt:'Validadores e métricas de staking.', ru:'Валидаторы и метрики стейкинга.', zh:'验证者与质押指标。', ja:'バリデーターとステーキング指標。', ko:'검증자 및 스테이킹 지표.', ar:'المدققون ومقاييس الرهن.', he:'ולידטורים ומדדי נעילה.', ur:'ویلیڈیٹرز اور اسٹیکنگ میٹرکس۔', hi:'वैलिडेटर और स्टेकिंग मेट्रिक्स।' },
  'staking.tab.validators':  { en:'Validators',   es:'Validadores',   fr:'Validateurs',  de:'Validatoren',  it:'Validatori',   pt:'Validadores',  ru:'Валидаторы',   zh:'验证者',  ja:'バリデーター',  ko:'검증자',    ar:'المدققون',   he:'ולידטורים',  ur:'ویلیڈیٹرز',  hi:'वैलिडेटर' },
  'staking.tab.network':     { en:'Network Info', es:'Info de la Red', fr:'Info réseau',  de:'Netzwerk-Info', it:'Info rete',   pt:'Info da rede', ru:'Сеть',         zh:'网络信息', ja:'ネット情報',    ko:'네트워크 정보', ar:'معلومات الشبكة', he:'מידע רשת', ur:'نیٹ ورک معلومات', hi:'नेटवर्क जानकारी' },

  /* ===== Governance (headers only; sub-tabs stay in Spanish) ===== */
  'gov.title':       { en:'Governance',  es:'Gobernanza',  fr:'Gouvernance',  de:'Governance',  it:'Governance',  pt:'Governança',  ru:'Управление',  zh:'治理',  ja:'ガバナンス',  ko:'거버넌스',  ar:'الحوكمة',  he:'ממשל',  ur:'گورننس',  hi:'शासन' },
  'gov.sub':         { en:'Council, Democracy and Technical Committee of the SORA chain.', es:'Consejo, Democracia y Comité Técnico de la cadena SORA.', fr:'Conseil, Démocratie et Comité Technique de la chaîne SORA.', de:'Council, Democracy und Technical Committee der SORA-Chain.', it:'Consiglio, Democrazia e Comitato Tecnico della chain SORA.', pt:'Conselho, Democracia e Comitê Técnico da chain SORA.', ru:'Совет, Демократия и Технический комитет SORA.', zh:'SORA 链的理事会、民主与技术委员会。', ja:'SORA チェーンの評議会、民主制、技術委員会。', ko:'SORA 체인의 의회, 민주주의, 기술 위원회.', ar:'مجلس وديمقراطية ولجنة فنية لسلسلة SORA.', he:'מועצה, דמוקרטיה וועדה טכנית של שרשרת SORA.', ur:'SORA چین کا کونسل، جمہوریت اور تکنیکی کمیٹی۔', hi:'SORA चेन का काउंसिल, डेमोक्रेसी और तकनीकी समिति।' },

  /* ===== Portfolio / Balance ===== */
  'portfolio.title': { en:'Portfolio',  es:'Cartera',  fr:'Portefeuille',  de:'Portfolio',  it:'Portafoglio',  pt:'Carteira',  ru:'Портфель',  zh:'投资组合',  ja:'ポートフォリオ',  ko:'포트폴리오',  ar:'المحفظة',  he:'תיק',  ur:'پورٹ فولیو',  hi:'पोर्टफ़ोलियो' },
  'portfolio.sub':   { en:'Holdings across all watched wallets.', es:'Tenencias en todas las carteras observadas.', fr:'Avoirs sur tous les wallets suivis.', de:'Bestände aller beobachteten Wallets.', it:'Partecipazioni in tutti i wallet osservati.', pt:'Posições em todas as carteiras monitoradas.', ru:'Активы по всем отслеживаемым кошелькам.', zh:'所有监控钱包的持仓。', ja:'監視中のすべてのウォレットの保有状況。', ko:'모니터링 중인 모든 지갑의 보유 현황.', ar:'الحيازات في جميع المحافظ المراقبة.', he:'החזקות בכל הארנקים המנוטרים.', ur:'نگرانی میں تمام والٹس کے ہولڈنگز۔', hi:'देखे गए सभी वॉलेट्स में होल्डिंग।' },
  'balance.title':   { en:'Balance',  es:'Saldo',  fr:'Solde',  de:'Guthaben',  it:'Saldo',  pt:'Saldo',  ru:'Баланс',  zh:'余额',  ja:'残高',  ko:'잔액',  ar:'الرصيد',  he:'יתרה',  ur:'بیلنس',  hi:'शेष' },
  'balance.sub':     { en:'Wallet summary and recent activity.', es:'Resumen de cartera y actividad reciente.', fr:'Résumé du wallet et activité récente.', de:'Wallet-Zusammenfassung und Aktivität.', it:'Riepilogo wallet e attività recente.', pt:'Resumo da carteira e atividade recente.', ru:'Сводка кошелька и активность.', zh:'钱包摘要与近期活动。', ja:'ウォレット概要と最近の活動。', ko:'지갑 요약 및 최근 활동.', ar:'ملخص المحفظة والنشاط الأخير.', he:'סיכום ארנק ופעילות אחרונה.', ur:'والیٹ خلاصہ اور حالیہ سرگرمی۔', hi:'वॉलेट सारांश और हाल की गतिविधि।' },

  /* ===== Drill panel ===== */
  'drill.overview':  { en:'Overview',     es:'Resumen',     fr:'Aperçu',      de:'Überblick',   it:'Panoramica',  pt:'Visão geral',  ru:'Обзор',        zh:'概览',     ja:'概要',        ko:'개요',         ar:'نظرة عامة',   he:'סקירה',       ur:'جائزہ',        hi:'अवलोकन' },
  'drill.details':   { en:'Details',      es:'Detalles',    fr:'Détails',     de:'Details',     it:'Dettagli',    pt:'Detalhes',     ru:'Подробности',  zh:'详情',     ja:'詳細',        ko:'세부사항',     ar:'التفاصيل',    he:'פרטים',       ur:'تفصیلات',      hi:'विवरण' },
  'drill.from':      { en:'From',         es:'De',          fr:'De',          de:'Von',         it:'Da',          pt:'De',           ru:'От',           zh:'来自',     ja:'から',        ko:'발신',         ar:'من',          he:'מאת',         ur:'از',          hi:'से' },
  'drill.to':        { en:'To',           es:'Para',        fr:'Vers',        de:'An',          it:'A',           pt:'Para',         ru:'Кому',         zh:'至',       ja:'宛先',        ko:'수신',         ar:'إلى',         he:'אל',          ur:'بنام',        hi:'को' },
  'drill.amount':    { en:'Amount',       es:'Cantidad',    fr:'Montant',     de:'Betrag',      it:'Importo',     pt:'Valor',        ru:'Сумма',        zh:'金额',     ja:'金額',        ko:'금액',         ar:'المبلغ',      he:'סכום',        ur:'رقم',          hi:'राशि' },
  'drill.fee':       { en:'Fee',          es:'Comisión',    fr:'Frais',       de:'Gebühr',      it:'Commissione', pt:'Taxa',         ru:'Комиссия',     zh:'手续费',   ja:'手数料',      ko:'수수료',       ar:'الرسوم',      he:'עמלה',        ur:'فیس',          hi:'शुल्क' },
  'drill.timestamp': { en:'Timestamp',    es:'Marca temporal', fr:'Horodatage', de:'Zeitstempel', it:'Timestamp',  pt:'Carimbo',      ru:'Время',        zh:'时间戳',   ja:'タイムスタンプ', ko:'타임스탬프',    ar:'الطابع الزمني', he:'חותמת זמן',  ur:'ٹائم اسٹیمپ',  hi:'टाइमस्टैम्प' },
  'drill.block':     { en:'Block',        es:'Bloque',      fr:'Bloc',        de:'Block',       it:'Blocco',      pt:'Bloco',        ru:'Блок',         zh:'区块',     ja:'ブロック',    ko:'블록',         ar:'كتلة',        he:'בלוק',        ur:'بلاک',         hi:'ब्लॉक' },
  'drill.hash':      { en:'Hash',         es:'Hash',        fr:'Hash',        de:'Hash',        it:'Hash',        pt:'Hash',         ru:'Хэш',          zh:'哈希',     ja:'ハッシュ',    ko:'해시',         ar:'هاش',         he:'האש',         ur:'ہیش',          hi:'हैश' },
  'drill.status':    { en:'Status',       es:'Estado',      fr:'Statut',      de:'Status',      it:'Stato',       pt:'Status',       ru:'Статус',       zh:'状态',     ja:'ステータス',   ko:'상태',         ar:'الحالة',      he:'סטטוס',       ur:'حالت',         hi:'स्थिति' },

  /* ===== Tweaks ===== */
  'tweaks.title':    { en:'Tweaks',       es:'Ajustes',     fr:'Réglages',    de:'Einstellungen', it:'Regolazioni', pt:'Ajustes',   ru:'Настройки',    zh:'调整',     ja:'調整',        ko:'조정',         ar:'التعديلات',   he:'התאמות',      ur:'ترتیبات',      hi:'समायोजन' },
  'tweaks.section':  { en:'Section',      es:'Sección',     fr:'Section',     de:'Bereich',     it:'Sezione',     pt:'Seção',        ru:'Раздел',       zh:'分区',     ja:'セクション',   ko:'섹션',         ar:'القسم',       he:'מקטע',        ur:'سیکشن',        hi:'खंड' },
  'tweaks.density':  { en:'Density',      es:'Densidad',    fr:'Densité',     de:'Dichte',      it:'Densità',     pt:'Densidade',    ru:'Плотность',    zh:'密度',     ja:'密度',        ko:'밀도',         ar:'الكثافة',     he:'צפיפות',      ur:'کثافت',        hi:'घनत्व' },
  'tweaks.motion':   { en:'Motion',       es:'Movimiento',  fr:'Mouvement',   de:'Bewegung',    it:'Movimento',   pt:'Movimento',    ru:'Движение',     zh:'动画',     ja:'モーション',   ko:'모션',         ar:'الحركة',      he:'תנועה',       ur:'حرکت',         hi:'गति' },
  'tweaks.accent':   { en:'Accent',       es:'Acento',      fr:'Accent',      de:'Akzent',      it:'Accento',     pt:'Destaque',     ru:'Акцент',       zh:'主色',     ja:'アクセント',   ko:'강조색',       ar:'اللون المميز', he:'צבע הדגשה',  ur:'لہجہ',         hi:'एक्सेंट' },

  /* ===== Staking KPIs / headers ===== */
  'staking.kpi.activeValidators': { en:'Active Validators', es:'Validadores Activos', fr:'Validateurs actifs', de:'Aktive Validatoren', it:'Validatori attivi', pt:'Validadores ativos', ru:'Активные валидаторы', zh:'活跃验证者', ja:'アクティブバリデーター', ko:'활성 검증자', ar:'المدققون النشطون', he:'ולידטורים פעילים', ur:'ایکٹو ویلیڈیٹرز', hi:'सक्रिय वैलिडेटर' },
  'staking.kpi.totalStaked':      { en:'Total Staked', es:'Total en Stake', fr:'Total staké', de:'Gesamt gestaked', it:'Totale in stake', pt:'Total em stake', ru:'Всего в стейкинге', zh:'总质押', ja:'総ステーク', ko:'총 스테이킹', ar:'إجمالي الرهن', he:'סה"כ נעול', ur:'کل اسٹیک', hi:'कुल स्टेक' },
  'staking.kpi.avgCommission':    { en:'Avg Commission', es:'Comisión Media', fr:'Commission moy.', de:'Ø Kommission', it:'Commissione media', pt:'Comissão média', ru:'Ср. комиссия', zh:'平均佣金', ja:'平均手数料', ko:'평균 수수료', ar:'متوسط العمولة', he:'עמלה ממוצעת', ur:'اوسط کمیشن', hi:'औसत कमीशन' },
  'staking.kpi.nextEraIn':        { en:'Next Era In', es:'Próxima Era En', fr:'Prochaine ère', de:'Nächste Ära', it:'Prossima era', pt:'Próxima era', ru:'След. эра через', zh:'下一纪元', ja:'次のエラ', ko:'다음 에라', ar:'العصر التالي خلال', he:'עידן הבא בעוד', ur:'اگلا دور', hi:'अगला एरा' },
  'staking.kpi.targetWaiting':    { en:'target · waiting', es:'objetivo · espera', fr:'cible · attente', de:'Ziel · Warteliste', it:'obiettivo · attesa', pt:'alvo · aguardando', ru:'цель · ожидание', zh:'目标 · 等待中', ja:'目標・待機', ko:'목표 · 대기', ar:'الهدف · الانتظار', he:'יעד · המתנה', ur:'ہدف · انتظار', hi:'लक्ष्य · प्रतीक्षा' },
  'staking.kpi.activeSet':        { en:'active set', es:'conjunto activo', fr:'ensemble actif', de:'aktives Set', it:'set attivo', pt:'conjunto ativo', ru:'активный набор', zh:'活跃集合', ja:'アクティブセット', ko:'활성 세트', ar:'المجموعة النشطة', he:'קבוצה פעילה', ur:'ایکٹو سیٹ', hi:'सक्रिय सेट' },
  'staking.kpi.era':              { en:'era', es:'era', fr:'ère', de:'Ära', it:'era', pt:'era', ru:'эра', zh:'纪元', ja:'エラ', ko:'에라', ar:'حقبة', he:'עידן', ur:'دور', hi:'युग' },

  'staking.col.validator':   { en:'Validator',  es:'Validador',   fr:'Validateur',  de:'Validator',  it:'Validatore',  pt:'Validador',  ru:'Валидатор',  zh:'验证者',  ja:'バリデーター',  ko:'검증자',  ar:'المدقق',  he:'ולידטור',  ur:'ویلیڈیٹر',  hi:'वैलिडेटर' },
  'staking.col.totalStake':  { en:'Total Stake', es:'Stake Total', fr:'Stake total', de:'Gesamt-Stake', it:'Stake totale', pt:'Stake total', ru:'Общий стейк', zh:'总质押', ja:'総ステーク', ko:'총 스테이크', ar:'إجمالي الرهن', he:'נעילה כוללת', ur:'کل اسٹیک', hi:'कुल स्टेक' },
  'staking.col.own':         { en:'Own', es:'Propio', fr:'Propre', de:'Eigen', it:'Proprio', pt:'Próprio', ru:'Своё', zh:'自质押', ja:'自己', ko:'자기', ar:'ذاتي', he:'עצמי', ur:'ذاتی', hi:'अपना' },
  'staking.col.noms':        { en:'Noms', es:'Nom.', fr:'Nom.', de:'Nom.', it:'Nom.', pt:'Nom.', ru:'Ном.', zh:'提名', ja:'指名', ko:'지명', ar:'المرشحون', he:'ממנים', ur:'نامزد', hi:'नामांकन' },
  'staking.col.commission':  { en:'Commission', es:'Comisión', fr:'Commission', de:'Kommission', it:'Commissione', pt:'Comissão', ru:'Комиссия', zh:'佣金', ja:'手数料', ko:'수수료', ar:'العمولة', he:'עמלה', ur:'کمیشن', hi:'कमीशन' },
  'staking.col.eraPts':      { en:'Era Pts', es:'Puntos', fr:'Points', de:'Era-Pts', it:'Pt. Era', pt:'Pts Era', ru:'Очки эры', zh:'纪元积分', ja:'エラPt', ko:'에라 점수', ar:'نقاط الحقبة', he:"נק' עידן", ur:'دور پوائنٹس', hi:'एरा अंक' },
  'staking.col.status':      { en:'Status', es:'Estado', fr:'Statut', de:'Status', it:'Stato', pt:'Status', ru:'Статус', zh:'状态', ja:'ステータス', ko:'상태', ar:'الحالة', he:'סטטוס', ur:'حالت', hi:'स्थिति' },

  'status.active':       { en:'Active',       es:'Activo',      fr:'Actif',      de:'Aktiv',      it:'Attivo',      pt:'Ativo',      ru:'Активен',     zh:'活跃',    ja:'アクティブ',  ko:'활성',     ar:'نشط',       he:'פעיל',    ur:'ایکٹو',    hi:'सक्रिय' },
  'status.waiting':      { en:'Waiting',      es:'En Espera',   fr:'En attente', de:'Wartend',    it:'In attesa',   pt:'Aguardando', ru:'Ожидание',    zh:'等待中',  ja:'待機',       ko:'대기',     ar:'قيد الانتظار', he:'ממתין',  ur:'انتظار',   hi:'प्रतीक्षारत' },
  'status.oversub':      { en:'Oversub.',     es:'Sobresusc.',  fr:'Sur-abonné', de:'Überbucht',  it:'Sovrascritto',pt:'Sobrescrito',ru:'Переподписка',zh:'超额',   ja:'定員超',     ko:'초과',     ar:'مفرط الاشتراك', he:'עודף',  ur:'اوور سب',  hi:'ओवरसब' },

  /* ===== Tweaks ===== */
  'tweaks.visible':       { en:'Section', es:'Sección', fr:'Section', de:'Bereich', it:'Sezione', pt:'Seção', ru:'Раздел', zh:'区域', ja:'セクション', ko:'섹션', ar:'القسم', he:'מקטע', ur:'سیکشن', hi:'खंड' },
  'tweaks.chartType':     { en:'Chart type', es:'Tipo de gráfico', fr:'Type de graphique', de:'Diagrammtyp', it:'Tipo di grafico', pt:'Tipo de gráfico', ru:'Тип графика', zh:'图表类型', ja:'チャートタイプ', ko:'차트 유형', ar:'نوع الرسم', he:'סוג תרשים', ur:'چارٹ کی قسم', hi:'चार्ट प्रकार' },
  'tweaks.liveSpeed':     { en:'Live speed', es:'Velocidad en vivo', fr:'Vitesse live', de:'Live-Geschwindigkeit', it:'Velocità live', pt:'Velocidade ao vivo', ru:'Скорость потока', zh:'实时速度', ja:'ライブ速度', ko:'실시간 속도', ar:'السرعة المباشرة', he:'מהירות חי', ur:'براہ راست رفتار', hi:'लाइव गति' },

  /* ===== Buttons / misc ===== */
  'btn.exportCsv':   { en:'Export CSV',       es:'Exportar CSV',  fr:'Exporter CSV',  de:'CSV exportieren',  it:'Esporta CSV',  pt:'Exportar CSV',  ru:'Экспорт CSV',  zh:'导出 CSV',  ja:'CSV エクスポート',  ko:'CSV 내보내기',  ar:'تصدير CSV',  he:'ייצא CSV',  ur:'CSV برآمد',  hi:'CSV निर्यात' },
  'btn.streaming':   { en:'streaming',        es:'en directo',    fr:'en direct',     de:'live',             it:'in diretta',   pt:'em tempo real', ru:'эфир',         zh:'实时',      ja:'配信中',          ko:'스트리밍',     ar:'بث',         he:'משדר',       ur:'لائیو',      hi:'स्ट्रीमिंग' },
  'btn.fullExplorer':{ en:'Full Explorer',    es:'Explorador Completo', fr:'Explorateur complet', de:'Voller Explorer', it:'Explorer completo', pt:'Explorador completo', ru:'Полный обозреватель', zh:'完整浏览器', ja:'フルエクスプローラー', ko:'전체 탐색기', ar:'المستكشف الكامل', he:'סייר מלא', ur:'مکمل ایکسپلورر', hi:'पूर्ण एक्सप्लोरर' },
  'btn.provideLiquidity': { en:'+ Provide Liquidity', es:'+ Aportar Liquidez', fr:'+ Fournir de la liquidité', de:'+ Liquidität bereitstellen', it:'+ Fornisci liquidità', pt:'+ Fornecer liquidez', ru:'+ Добавить ликвидность', zh:'+ 提供流动性', ja:'+ 流動性を提供', ko:'+ 유동성 공급', ar:'+ توفير السيولة', he:'+ ספק נזילות', ur:'+ لکویڈٹی فراہم', hi:'+ लिक्विडिटी प्रदान करें' },
  'btn.addWallet':   { en:'+ Add wallet',     es:'+ Añadir cartera',  fr:'+ Ajouter un wallet', de:'+ Wallet hinzufügen', it:'+ Aggiungi wallet', pt:'+ Adicionar carteira', ru:'+ Добавить кошелёк', zh:'+ 添加钱包', ja:'+ ウォレット追加', ko:'+ 지갑 추가', ar:'+ إضافة محفظة', he:'+ הוסף ארנק', ur:'+ والیٹ شامل', hi:'+ वॉलेट जोड़ें' },

  /* ===== Filter chips / toggles ===== */
  'chip.all':        { en:'All',     es:'Todos',     fr:'Tous',     de:'Alle',     it:'Tutti',    pt:'Todos',   ru:'Все',     zh:'全部',  ja:'すべて',   ko:'전체',   ar:'الكل',    he:'הכול',   ur:'سب',      hi:'सभी' },
  'chip.swap':       { en:'Swap',    es:'Swap',      fr:'Swap',     de:'Swap',     it:'Swap',     pt:'Swap',    ru:'Своп',    zh:'兑换',  ja:'スワップ', ko:'스왑',   ar:'مبادلة',  he:'החלפה',  ur:'سواپ',    hi:'स्वैप' },
  'chip.transfer':   { en:'Transfer',es:'Transfer.', fr:'Transfert',de:'Transfer', it:'Trasf.',   pt:'Transf.', ru:'Перевод', zh:'转账',  ja:'送金',    ko:'전송',   ar:'تحويل',   he:'העברה',  ur:'منتقلی',  hi:'ट्रांस.' },
  'chip.block':      { en:'Block',   es:'Bloque',    fr:'Bloc',     de:'Block',    it:'Blocco',   pt:'Bloco',   ru:'Блок',    zh:'区块',  ja:'ブロック', ko:'블록',   ar:'كتلة',    he:'בלוק',   ur:'بلاک',    hi:'ब्लॉक' },
  'chip.order':      { en:'Order',   es:'Orden',     fr:'Ordre',    de:'Order',    it:'Ordine',   pt:'Ordem',   ru:'Ордер',   zh:'订单',  ja:'注文',    ko:'주문',   ar:'أمر',     he:'הזמנה',  ur:'آرڈر',    hi:'ऑर्डर' },
  'chip.burn':       { en:'Burn',    es:'Quema',     fr:'Burn',     de:'Burn',     it:'Burn',     pt:'Burn',    ru:'Сжигание',zh:'销毁',  ja:'バーン',   ko:'소각',   ar:'حرق',     he:'שריפה',  ur:'برن',     hi:'बर्न' },
  'chip.favorites':  { en:'Favorites', es:'Favoritos', fr:'Favoris', de:'Favoriten', it:'Preferiti', pt:'Favoritos', ru:'Избранное', zh:'收藏', ja:'お気に入り', ko:'즐겨찾기', ar:'المفضلة', he:'מועדפים', ur:'پسندیدہ', hi:'पसंदीदा' },

  /* ===== Pulse ===== */
  'pulse.kpi.swaps24':    { en:'Swaps · 24h',     es:'Swaps · 24h',      fr:'Swaps · 24h',      de:'Swaps · 24h',      it:'Swap · 24h',      pt:'Swaps · 24h',     ru:'Свопы · 24ч',      zh:'兑换 · 24h',  ja:'スワップ・24h',  ko:'스왑 · 24h',   ar:'مبادلات · 24س',   he:'החלפות · 24h', ur:'سواپس · 24h',  hi:'स्वैप · 24h' },
  'pulse.kpi.volume':     { en:'Volume · KUSD',   es:'Volumen · KUSD',   fr:'Volume · KUSD',    de:'Volumen · KUSD',   it:'Volume · KUSD',   pt:'Volume · KUSD',   ru:'Объём · KUSD',     zh:'交易量 · KUSD', ja:'取引高・KUSD',   ko:'거래량 · KUSD',ar:'الحجم · KUSD',    he:'נפח · KUSD',  ur:'حجم · KUSD',   hi:'वॉल्यूम · KUSD' },
  'pulse.kpi.wallets':    { en:'Active Wallets',  es:'Carteras Activas', fr:'Wallets actifs',   de:'Aktive Wallets',   it:'Wallet attivi',   pt:'Carteiras ativas',ru:'Активные кошельки',zh:'活跃钱包',    ja:'アクティブウォレット', ko:'활성 지갑', ar:'المحافظ النشطة', he:'ארנקים פעילים', ur:'ایکٹو والٹس', hi:'सक्रिय वॉलेट' },
  'pulse.kpi.block':      { en:'Avg Block Time',  es:'Tiempo Medio de Bloque', fr:'Temps moy. de bloc', de:'Ø Blockzeit', it:'Tempo medio blocco', pt:'Tempo méd. de bloco', ru:'Ср. время блока', zh:'平均出块', ja:'平均ブロック時間', ko:'평균 블록 시간', ar:'متوسط وقت الكتلة', he:'זמן בלוק ממוצע', ur:'اوسط بلاک ٹائم', hi:'औसत ब्लॉक समय' },
  'pulse.trending':       { en:'Trending Tokens · 24h', es:'Tokens en Tendencia · 24h', fr:'Tokens tendance · 24h', de:'Trending-Tokens · 24h', it:'Token in trend · 24h', pt:'Tokens em alta · 24h', ru:'Трендовые токены · 24ч', zh:'热门代币 · 24h', ja:'トレンドトークン・24h', ko:'트렌딩 토큰 · 24h', ar:'التوكنز الرائجة · 24س', he:'טוקנים חמים · 24h', ur:'ٹرینڈنگ ٹوکنز · 24h', hi:'ट्रेंडिंग टोकन · 24h' },
  'pulse.health':         { en:'Network Health',  es:'Salud de la Red',  fr:'Santé du réseau',  de:'Netzwerk-Zustand', it:'Stato di rete',   pt:'Saúde da rede',   ru:'Состояние сети',   zh:'网络健康',    ja:'ネットワーク健全性', ko:'네트워크 상태', ar:'صحة الشبكة',      he:'בריאות הרשת',  ur:'نیٹ ورک صحت',  hi:'नेटवर्क स्वास्थ्य' },

  /* ===== Burn KPIs ===== */
  'burn.kpi.totalBurned': { en:'Total Burned', es:'Total Quemado', fr:'Total brûlé', de:'Gesamt verbrannt', it:'Totale bruciato', pt:'Total queimado', ru:'Всего сожжено', zh:'总销毁', ja:'総バーン', ko:'총 소각', ar:'إجمالي الحرق', he:'סה"כ נשרף', ur:'کل برن', hi:'कुल बर्न' },
  'burn.kpi.burnRate':    { en:'Burn Rate',    es:'Tasa de Quema', fr:'Taux de burn', de:'Burn-Rate',   it:'Tasso di burn',   pt:'Taxa de queima', ru:'Скорость сжигания', zh:'销毁速率', ja:'バーンレート', ko:'소각 속도', ar:'معدل الحرق', he:'קצב שריפה', ur:'برن ریٹ', hi:'बर्न दर' },
  'burn.kpi.supply':      { en:'Circulating',  es:'Circulante',   fr:'En circulation', de:'Im Umlauf',  it:'Circolante',     pt:'Em circulação', ru:'В обращении', zh:'流通量', ja:'流通量', ko:'유통량', ar:'المتداول', he:'במחזור', ur:'گردش میں', hi:'परिसंचरण' },
  'burn.kpi.priceImpact': { en:'Price · 24h',  es:'Precio · 24h', fr:'Prix · 24h',   de:'Preis · 24h', it:'Prezzo · 24h',   pt:'Preço · 24h',   ru:'Цена · 24ч', zh:'价格 · 24h', ja:'価格・24h', ko:'가격 · 24h', ar:'السعر · 24س', he:'מחיר · 24h', ur:'قیمت · 24h', hi:'मूल्य · 24h' },
  'burn.topHolders':      { en:'Top Holders',  es:'Principales Titulares', fr:'Principaux détenteurs', de:'Top-Halter', it:'Principali detentori', pt:'Principais detentores', ru:'Топ-держатели', zh:'主要持有者', ja:'トップホルダー', ko:'상위 보유자', ar:'كبار الحائزين', he:'מחזיקים מובילים', ur:'ٹاپ ہولڈرز', hi:'शीर्ष धारक' },
  'burn.burnRateCum':     { en:'Burn Rate — cumulative', es:'Tasa de quema — acumulada', fr:'Taux de burn — cumulé', de:'Burn-Rate — kumuliert', it:'Tasso di burn — cumulativo', pt:'Taxa de queima — acumulada', ru:'Скорость сжигания — нарастающая', zh:'销毁速率 — 累计', ja:'バーンレート — 累積', ko:'소각 속도 — 누적', ar:'معدل الحرق — تراكمي', he:'קצב שריפה — מצטבר', ur:'برن ریٹ — مجموعی', hi:'बर्न दर — संचयी' },

  /* ===== Swaps / Extrinsics / Transfers / Bridges / Holders table ===== */
  'col.time':        { en:'Time',    es:'Tiempo',    fr:'Temps',   de:'Zeit',     it:'Ora',     pt:'Hora',     ru:'Время',    zh:'时间',  ja:'時間',   ko:'시간',    ar:'الوقت',    he:'זמן',     ur:'وقت',      hi:'समय' },
  'col.from':        { en:'From',    es:'De',        fr:'De',      de:'Von',      it:'Da',      pt:'De',       ru:'От',       zh:'来自',  ja:'から',   ko:'발신',    ar:'من',       he:'מאת',     ur:'از',       hi:'से' },
  'col.to':          { en:'To',      es:'Para',      fr:'Vers',    de:'An',       it:'A',       pt:'Para',     ru:'Кому',     zh:'至',    ja:'宛先',   ko:'수신',    ar:'إلى',      he:'אל',      ur:'بنام',     hi:'को' },
  'col.rate':        { en:'Rate',    es:'Tasa',      fr:'Taux',    de:'Kurs',     it:'Tasso',   pt:'Taxa',     ru:'Курс',     zh:'汇率',  ja:'レート',  ko:'환율',    ar:'السعر',    he:'שער',     ur:'ریٹ',      hi:'दर' },
  'col.fee':         { en:'Fee',     es:'Comisión',  fr:'Frais',   de:'Gebühr',   it:'Commiss.',pt:'Taxa',     ru:'Комиссия', zh:'手续费',ja:'手数料',  ko:'수수료',  ar:'رسوم',     he:'עמלה',    ur:'فیس',      hi:'शुल्क' },
  'col.caller':      { en:'Caller',  es:'Llamante',  fr:'Appelant',de:'Aufrufer', it:'Chiamante',pt:'Chamador',ru:'Отправитель',zh:'调用者',ja:'呼び出し元', ko:'호출자', ar:'المتصل',   he:'קורא',    ur:'کالر',     hi:'कॉलर' },
  'col.status':      { en:'Status',  es:'Estado',    fr:'Statut',  de:'Status',   it:'Stato',   pt:'Status',   ru:'Статус',   zh:'状态',  ja:'ステータス',ko:'상태',  ar:'الحالة',   he:'סטטוס',   ur:'حالت',     hi:'स्थिति' },
  'col.asset':       { en:'Asset',   es:'Activo',    fr:'Actif',   de:'Asset',    it:'Asset',   pt:'Ativo',    ru:'Актив',    zh:'资产',  ja:'資産',    ko:'자산',    ar:'الأصل',    he:'נכס',     ur:'اثاثہ',    hi:'परिसंपत्ति' },
  'col.amount':      { en:'Amount',  es:'Cantidad',  fr:'Montant', de:'Betrag',   it:'Importo', pt:'Valor',    ru:'Сумма',    zh:'数量',  ja:'金額',    ko:'금액',    ar:'المبلغ',   he:'סכום',    ur:'رقم',      hi:'राशि' },
  'col.memo':        { en:'Memo',    es:'Nota',      fr:'Note',    de:'Notiz',    it:'Nota',    pt:'Nota',     ru:'Примеч.',  zh:'备注',  ja:'メモ',    ko:'메모',    ar:'ملاحظة',   he:'הערה',    ur:'نوٹ',      hi:'मेमो' },
  'col.direction':   { en:'Direction',es:'Dirección',fr:'Direction',de:'Richtung',it:'Direzione',pt:'Direção', ru:'Направление',zh:'方向',ja:'方向',    ko:'방향',    ar:'الاتجاه',   he:'כיוון',  ur:'سمت',      hi:'दिशा' },
  'col.chain':       { en:'Chain',   es:'Cadena',    fr:'Chaîne',  de:'Chain',    it:'Chain',   pt:'Cadeia',   ru:'Сеть',     zh:'链',    ja:'チェーン', ko:'체인',    ar:'السلسلة',  he:'שרשרת',  ur:'چین',     hi:'चेन' },
  'col.pair':        { en:'Pair',    es:'Par',       fr:'Paire',   de:'Paar',     it:'Coppia',  pt:'Par',      ru:'Пара',     zh:'交易对',ja:'ペア',    ko:'페어',    ar:'زوج',      he:'צמד',     ur:'جوڑا',    hi:'जोड़ी' },
  'col.price':       { en:'Price',   es:'Precio',    fr:'Prix',    de:'Preis',    it:'Prezzo',  pt:'Preço',    ru:'Цена',     zh:'价格',  ja:'価格',    ko:'가격',    ar:'السعر',    he:'מחיר',    ur:'قیمت',    hi:'मूल्य' },
  'col.size':        { en:'Size',    es:'Tamaño',    fr:'Taille',  de:'Größe',    it:'Dim.',    pt:'Tamanho',  ru:'Размер',   zh:'规模',  ja:'サイズ',  ko:'크기',    ar:'الحجم',    he:'גודל',    ur:'سائز',    hi:'आकार' },
  'col.total':       { en:'Total',   es:'Total',     fr:'Total',   de:'Gesamt',   it:'Totale',  pt:'Total',    ru:'Итого',    zh:'总额',  ja:'合計',    ko:'합계',    ar:'الإجمالي', he:'סה"כ',   ur:'کل',      hi:'कुल' },
  'col.rank':        { en:'Rank',    es:'Rango',     fr:'Rang',    de:'Rang',     it:'Rango',   pt:'Posição',  ru:'Ранг',     zh:'排名',  ja:'順位',    ko:'순위',    ar:'الترتيب',   he:'דירוג',  ur:'رینک',    hi:'रैंक' },
  'col.account':     { en:'Account', es:'Cuenta',    fr:'Compte',  de:'Konto',    it:'Account', pt:'Conta',    ru:'Аккаунт',  zh:'账户',  ja:'アカウント',ko:'계정',  ar:'الحساب',   he:'חשבון',   ur:'اکاؤنٹ',  hi:'खाता' },
  'col.value':       { en:'Value',   es:'Valor',     fr:'Valeur',  de:'Wert',     it:'Valore',  pt:'Valor',    ru:'Стоимость',zh:'价值',  ja:'価値',    ko:'가치',    ar:'القيمة',    he:'ערך',    ur:'ویلیو',   hi:'मूल्य' },
  'col.tokens':      { en:'Tokens',  es:'Tokens',    fr:'Tokens',  de:'Tokens',   it:'Token',   pt:'Tokens',   ru:'Токены',   zh:'代币',  ja:'トークン', ko:'토큰',    ar:'العملات',  he:'אסימונים',ur:'ٹوکنز',  hi:'टोकन' },
  'col.lastActivity':{ en:'Last Activity', es:'Última Actividad', fr:'Dern. activité', de:'Letzte Aktivität', it:'Ultima attività', pt:'Última atividade', ru:'Последняя активность', zh:'最后活动', ja:'最終活動', ko:'마지막 활동', ar:'آخر نشاط', he:'פעילות אחרונה', ur:'آخری سرگرمی', hi:'अंतिम गतिविधि' },
  'col.pool':        { en:'Pool',    es:'Pool',      fr:'Pool',    de:'Pool',     it:'Pool',    pt:'Pool',     ru:'Пул',      zh:'池',    ja:'プール',   ko:'풀',      ar:'المجمع',   he:'בריכה',   ur:'پول',     hi:'पूल' },
  'col.tvl':         { en:'TVL',     es:'TVL',       fr:'TVL',     de:'TVL',      it:'TVL',     pt:'TVL',      ru:'TVL',      zh:'TVL',   ja:'TVL',     ko:'TVL',    ar:'TVL',      he:'TVL',    ur:'TVL',     hi:'TVL' },
  'col.volume':      { en:'Volume',  es:'Volumen',   fr:'Volume',  de:'Volumen',  it:'Volume',  pt:'Volume',   ru:'Объём',    zh:'交易量',ja:'取引高',  ko:'거래량',  ar:'الحجم',    he:'נפח',     ur:'حجم',     hi:'वॉल्यूम' },
  'col.apr':         { en:'APR',     es:'APR',       fr:'APR',     de:'APR',      it:'APR',     pt:'APR',      ru:'APR',      zh:'年化',  ja:'APR',     ko:'APR',    ar:'APR',      he:'APR',    ur:'APR',     hi:'APR' },
  'col.change24':    { en:'24h',     es:'24h',       fr:'24h',     de:'24h',      it:'24h',     pt:'24h',      ru:'24ч',      zh:'24h',   ja:'24h',     ko:'24h',    ar:'24س',      he:'24h',    ur:'24h',     hi:'24h' },
  'col.marketCap':   { en:'Market Cap', es:'Cap. de Mercado', fr:'Cap. marché', de:'Marktkap.', it:'Cap. mercato', pt:'Cap. de mercado', ru:'Рын. кап.', zh:'市值',  ja:'時価総額', ko:'시가총액',ar:'القيمة السوقية', he:'שווי שוק', ur:'مارکیٹ کیپ', hi:'मार्केट कैप' },
  'col.supply':      { en:'Supply',  es:'Oferta',    fr:'Offre',   de:'Angebot',  it:'Offerta', pt:'Oferta',   ru:'Предложение',zh:'供应',ja:'供給量',  ko:'공급',   ar:'الإمداد',   he:'היצע',   ur:'سپلائی',  hi:'आपूर्ति' },
  'col.extrinsic':   { en:'Extrinsic', es:'Extrínseco', fr:'Extrinsèque', de:'Extrinsic', it:'Extrinsic', pt:'Extrínseco', ru:'Экстринсик', zh:'外部交易', ja:'エクストリンシック', ko:'익스트린식', ar:'المعاملة', he:'אקסטרינסיק', ur:'ایکسٹرنسک', hi:'एक्सट्रिंसिक' },
  'col.call':        { en:'Call',    es:'Llamada',   fr:'Appel',   de:'Aufruf',   it:'Chiamata',pt:'Chamada',  ru:'Вызов',    zh:'调用',  ja:'呼び出し', ko:'호출',    ar:'الاستدعاء',he:'קריאה',  ur:'کال',     hi:'कॉल' },
  'col.signer':      { en:'Signer',  es:'Firmante',  fr:'Signataire',de:'Signierer',it:'Firmatario',pt:'Signatário',ru:'Подписант',zh:'签名者',ja:'署名者',ko:'서명자',ar:'الموقّع', he:'חותם',   ur:'دستخط کنندہ',hi:'हस्ताक्षरक' },
  'col.result':      { en:'Result',  es:'Resultado', fr:'Résultat',de:'Ergebnis', it:'Risultato',pt:'Resultado',ru:'Результат',zh:'结果',ja:'結果',    ko:'결과',    ar:'النتيجة',  he:'תוצאה',  ur:'نتیجہ',  hi:'परिणाम' },

  /* ===== Status pills ===== */
  'status.success':  { en:'Success',  es:'Éxito',   fr:'Succès',  de:'Erfolg',  it:'Successo',pt:'Sucesso', ru:'Успех',   zh:'成功',  ja:'成功',    ko:'성공',   ar:'نجاح',     he:'הצלחה',  ur:'کامیاب',  hi:'सफल' },
  'status.failed':   { en:'Failed',   es:'Fallido', fr:'Échec',   de:'Fehlgeschl.',it:'Fallito',pt:'Falhou',  ru:'Ошибка',  zh:'失败',  ja:'失敗',    ko:'실패',   ar:'فشل',      he:'נכשל',   ur:'ناکام',   hi:'विफल' },
  'status.pending':  { en:'Pending',  es:'Pendiente',fr:'En cours',de:'Ausstehend',it:'In attesa',pt:'Pendente',ru:'Ожидание',zh:'待处理',ja:'保留中',  ko:'대기중', ar:'قيد الانتظار',he:'ממתין', ur:'زیر التوا',hi:'लंबित' },
  'status.done':     { en:'Done',     es:'Hecho',   fr:'Terminé', de:'Fertig',  it:'Fatto',   pt:'Feito',   ru:'Готово',  zh:'已完成',ja:'完了',    ko:'완료',   ar:'تم',       he:'בוצע',   ur:'مکمل',    hi:'हो गया' },
  'status.finalized':{ en:'Finalized',es:'Finalizado',fr:'Finalisé',de:'Finalisiert',it:'Finalizzato',pt:'Finalizado',ru:'Финализирован',zh:'已最终化',ja:'ファイナライズ',ko:'최종화됨',ar:'مُنهى',he:'סופי',ur:'حتمی',hi:'अंतिम' },
  'sev.low':         { en:'Low',      es:'Baja',    fr:'Faible',  de:'Niedrig', it:'Bassa',   pt:'Baixa',   ru:'Низкая',  zh:'低',    ja:'低',      ko:'낮음',   ar:'منخفض',    he:'נמוכה',  ur:'کم',      hi:'निम्न' },
  'sev.medium':      { en:'Medium',   es:'Media',   fr:'Moyenne', de:'Mittel',  it:'Media',   pt:'Média',   ru:'Средняя', zh:'中',    ja:'中',      ko:'보통',   ar:'متوسط',    he:'בינונית',ur:'درمیانہ',hi:'मध्यम' },
  'sev.high':        { en:'High',     es:'Alta',    fr:'Élevée',  de:'Hoch',    it:'Alta',    pt:'Alta',    ru:'Высокая', zh:'高',    ja:'高',      ko:'높음',   ar:'عالي',     he:'גבוהה',  ur:'بلند',    hi:'उच्च' },
  'sev.critical':    { en:'Critical', es:'Crítica', fr:'Critique',de:'Kritisch',it:'Critica', pt:'Crítica', ru:'Критич.', zh:'严重',  ja:'重大',    ko:'심각',   ar:'حرج',      he:'קריטית', ur:'نازک',    hi:'गंभीर' },

  /* ===== Pagination ===== */
  'pag.first':       { en:'« First',  es:'« Inicio', fr:'« Début', de:'« Erste', it:'« Prima', pt:'« Início',ru:'« В начало',zh:'« 首页',ja:'« 最初',  ko:'« 처음', ar:'« الأول',  he:'« ראשון',ur:'« پہلا',  hi:'« प्रथम' },
  'pag.prev':        { en:'⬅ Prev',   es:'⬅ Ant.',  fr:'⬅ Préc.', de:'⬅ Zurück',it:'⬅ Prec.',pt:'⬅ Ant.',  ru:'⬅ Назад', zh:'⬅ 上一页',ja:'⬅ 前へ',  ko:'⬅ 이전', ar:'⬅ السابق', he:'⬅ הקודם', ur:'⬅ پچھلا', hi:'⬅ पिछला' },
  'pag.next':        { en:'Next ➡',   es:'Sig. ➡',  fr:'Suiv. ➡', de:'Weiter ➡',it:'Succ. ➡',pt:'Próx. ➡', ru:'Далее ➡', zh:'下一页 ➡',ja:'次へ ➡',  ko:'다음 ➡', ar:'التالي ➡', he:'הבא ➡',  ur:'اگلا ➡',  hi:'अगला ➡' },
  'pag.last':        { en:'Last »',   es:'Último »',fr:'Fin »',   de:'Letzte »',it:'Ultima »',pt:'Último »',ru:'В конец »',zh:'末页 »',  ja:'最後 »',  ko:'마지막 »',ar:'الأخير »',he:'אחרון »', ur:'آخری »',  hi:'अंतिम »' },
  'pag.pageOf':      { en:'Page',     es:'Página',  fr:'Page',    de:'Seite',   it:'Pagina',  pt:'Página',  ru:'Стр.',    zh:'第',    ja:'ページ',  ko:'페이지', ar:'صفحة',     he:'עמוד',   ur:'صفحہ',    hi:'पृष्ठ' },
  'pag.of':          { en:'of',       es:'de',      fr:'sur',     de:'von',     it:'di',      pt:'de',      ru:'из',      zh:'/',     ja:'/',       ko:'/',      ar:'من',       he:'מתוך',   ur:'از',      hi:'का' },

  /* ===== Governance inner labels ===== */
  'gov.col.member':     { en:'Member',    es:'Miembro',    fr:'Membre',     de:'Mitglied',   it:'Membro',    pt:'Membro',    ru:'Член',       zh:'成员',  ja:'メンバー',  ko:'구성원',  ar:'عضو',      he:'חבר',    ur:'رکن',     hi:'सदस्य' },
  'gov.col.candidate':  { en:'Candidate', es:'Candidato',  fr:'Candidat',   de:'Kandidat',   it:'Candidato', pt:'Candidato', ru:'Кандидат',   zh:'候选人', ja:'候補者',    ko:'후보',    ar:'مرشح',     he:'מועמד',  ur:'امیدوار',  hi:'उम्मीदवार' },
  'gov.col.motion':     { en:'Motion',    es:'Moción',     fr:'Motion',     de:'Antrag',     it:'Mozione',   pt:'Moção',     ru:'Предложение',zh:'动议',  ja:'動議',      ko:'동의',    ar:'اقتراح',   he:'הצעה',   ur:'تحریک',   hi:'प्रस्ताव' },
  'gov.col.proposal':   { en:'Proposal',  es:'Propuesta',  fr:'Proposition',de:'Vorschlag',  it:'Proposta',  pt:'Proposta',  ru:'Предложение',zh:'提案',  ja:'提案',      ko:'제안',    ar:'مقترح',    he:'הצעה',   ur:'تجویز',   hi:'प्रस्ताव' },
  'gov.col.votes':      { en:'Votes',     es:'Votos',      fr:'Votes',      de:'Stimmen',    it:'Voti',      pt:'Votos',     ru:'Голоса',     zh:'投票',  ja:'投票',      ko:'투표',    ar:'الأصوات',  he:'הצבעות', ur:'ووٹ',     hi:'वोट' },
  'gov.col.aye':        { en:'AYE',       es:'SÍ',         fr:'POUR',       de:'JA',         it:'SÌ',        pt:'SIM',       ru:'ЗА',         zh:'赞成',  ja:'賛成',      ko:'찬성',    ar:'نعم',      he:'בעד',    ur:'ہاں',     hi:'हाँ' },
  'gov.col.nay':        { en:'NAY',       es:'NO',         fr:'CONTRE',     de:'NEIN',       it:'NO',        pt:'NÃO',       ru:'ПРОТИВ',     zh:'反对',  ja:'反対',      ko:'반대',    ar:'لا',       he:'נגד',    ur:'نہیں',    hi:'नहीं' },
  'gov.col.deadline':   { en:'Deadline',  es:'Plazo',      fr:'Échéance',   de:'Frist',      it:'Scadenza',  pt:'Prazo',     ru:'Срок',       zh:'截止',  ja:'期限',      ko:'마감',    ar:'الموعد',   he:'יעד',    ur:'آخری تاریخ',hi:'समय सीमा' },
  'gov.col.proposer':   { en:'Proposer',  es:'Proponente', fr:'Proposant',  de:'Antragst.',  it:'Proponente',pt:'Proponente',ru:'Автор',      zh:'提议者',ja:'提案者',    ko:'제안자',  ar:'المقترح',  he:'מציע',   ur:'تجویز کنندہ',hi:'प्रस्तावक' },
  'gov.col.threshold':  { en:'Threshold', es:'Umbral',     fr:'Seuil',      de:'Schwelle',   it:'Soglia',    pt:'Limiar',    ru:'Порог',      zh:'阈值',  ja:'しきい値',  ko:'임계값',  ar:'العتبة',   he:'סף',     ur:'حد',      hi:'सीमा' },

  /* ===== Balance ===== */
  'balance.netWorth':   { en:'Net Worth',    es:'Patrimonio Neto', fr:'Valeur nette', de:'Nettowert',   it:'Patrimonio netto',pt:'Patrimônio líq.',ru:'Чистая стоимость',zh:'净值',ja:'純資産',  ko:'순자산',  ar:'صافي الثروة',he:'שווי נטו',  ur:'خالص مالیت',hi:'कुल संपत्ति' },
  'balance.allocation': { en:'Allocation',   es:'Asignación',     fr:'Allocation',   de:'Allokation',  it:'Allocazione',    pt:'Alocação',        ru:'Распределение',   zh:'资产分配',ja:'配分',    ko:'자산 배분',ar:'التخصيص',  he:'הקצאה',   ur:'تقسیم',   hi:'आवंटन' },
  'balance.change24':   { en:'24h Change',   es:'Cambio 24h',     fr:'Variation 24h',de:'24h-Änderung',it:'Variazione 24h', pt:'Variação 24h',    ru:'Изменение 24ч',   zh:'24h 变动',ja:'24h 変動', ko:'24h 변동',ar:'تغير 24س', he:'שינוי 24h', ur:'24h تبدیلی',hi:'24h बदलाव' },
  'balance.addToast':   { en:'Wallet address input coming soon', es:'Entrada de dirección de cartera próximamente', fr:'Entrée d\'adresse bientôt', de:'Wallet-Adresse bald verfügbar', it:'Input indirizzo in arrivo', pt:'Entrada de endereço em breve', ru:'Ввод адреса скоро', zh:'钱包地址输入即将推出', ja:'ウォレットアドレス入力は近日公開', ko:'지갑 주소 입력 곧 제공', ar:'إدخال عنوان المحفظة قريباً', he:'קלט כתובת ארנק בקרוב', ur:'والٹ ایڈریس جلد', hi:'वॉलेट पता जल्द' },

  /* ===== Intelligence ===== */
  'intel.kpi.insights': { en:'Insights · 24h', es:'Perspectivas · 24h', fr:'Analyses · 24h', de:'Einblicke · 24h', it:'Insight · 24h', pt:'Insights · 24h', ru:'Аналитика · 24ч', zh:'洞察 · 24h', ja:'洞察・24h', ko:'인사이트 · 24h', ar:'رؤى · 24س', he:'תובנות · 24h', ur:'بصیرتیں · 24h', hi:'अंतर्दृष्टि · 24h' },
  'intel.kpi.alerts':   { en:'Active Alerts',  es:'Alertas Activas',   fr:'Alertes actives',de:'Aktive Warnungen',it:'Avvisi attivi', pt:'Alertas ativos', ru:'Активные алерты', zh:'活跃警报', ja:'アクティブアラート', ko:'활성 알림', ar:'تنبيهات نشطة', he:'התרעות פעילות', ur:'ایکٹو الرٹس', hi:'सक्रिय अलर्ट' },
  'intel.kpi.watchlist':{ en:'Watchlist Hits', es:'Lista de Vigilancia',fr:'Watchlist',     de:'Watchlist-Treffer',it:'Hit watchlist', pt:'Watchlist',      ru:'Срабатывания',    zh:'关注命中', ja:'ウォッチリスト', ko:'워치리스트', ar:'قائمة المراقبة', he:'רשימת מעקב', ur:'واچ لسٹ', hi:'वॉचलिस्ट' },
  'intel.kpi.anomalies':{ en:'Open Anomalies', es:'Anomalías Abiertas',fr:'Anomalies ouv.',de:'Offene Anomalien',it:'Anomalie aperte',pt:'Anomalias abertas',ru:'Открытые аномалии',zh:'未解决异常',ja:'未解決異常',ko:'미해결 이상',ar:'حالات شاذة',he:'חריגות פתוחות',ur:'کھلی بے ضابطگیاں',hi:'खुली विसंगतियाँ' },

  /* ===== Music player ===== */
  'music.nowPlaying':{ en:'Now playing', es:'Reproduciendo', fr:'En lecture', de:'Läuft jetzt', it:'In riproduzione', pt:'Tocando agora', ru:'Сейчас играет', zh:'正在播放', ja:'再生中', ko:'재생 중', ar:'قيد التشغيل', he:'מתנגן כעת', ur:'چل رہا ہے', hi:'अभी चल रहा' },
  'music.track':     { en:'Track',       es:'Pista',        fr:'Piste',      de:'Titel',      it:'Traccia',        pt:'Faixa',        ru:'Трек',          zh:'曲目',   ja:'トラック', ko:'트랙',   ar:'المسار',     he:'רצועה',    ur:'ٹریک',    hi:'ट्रैक' },
  'music.artist':    { en:'Artist',      es:'Artista',      fr:'Artiste',    de:'Künstler',   it:'Artista',        pt:'Artista',      ru:'Исполнитель',   zh:'艺术家', ja:'アーティスト',ko:'아티스트',ar:'الفنان',     he:'אמן',      ur:'فنکار',   hi:'कलाकार' },
  'music.playlist':  { en:'Playlist',    es:'Lista',        fr:'Playlist',   de:'Playlist',   it:'Playlist',       pt:'Playlist',     ru:'Плейлист',      zh:'播放列表',ja:'プレイリスト',ko:'플레이리스트',ar:'قائمة التشغيل',he:'פלייליסט',ur:'پلے لسٹ', hi:'प्लेलिस्ट' },
  'music.volume':    { en:'Volume',      es:'Volumen',      fr:'Volume',     de:'Lautstärke', it:'Volume',         pt:'Volume',       ru:'Громкость',     zh:'音量',   ja:'音量',    ko:'볼륨',    ar:'الصوت',      he:'עוצמה',    ur:'والیوم',  hi:'वॉल्यूम' },
  'music.play':      { en:'Play',        es:'Reproducir',   fr:'Lire',       de:'Abspielen',  it:'Riproduci',      pt:'Tocar',        ru:'Играть',        zh:'播放',   ja:'再生',    ko:'재생',    ar:'تشغيل',      he:'נגן',      ur:'چلائیں',  hi:'चलाएँ' },
  'music.pause':     { en:'Pause',       es:'Pausar',       fr:'Pause',      de:'Pause',      it:'Pausa',          pt:'Pausar',       ru:'Пауза',         zh:'暂停',   ja:'一時停止',ko:'일시정지',ar:'إيقاف مؤقت', he:'השהה',    ur:'روکیں',   hi:'रोकें' },
  'music.next':      { en:'Next',        es:'Siguiente',    fr:'Suivant',    de:'Weiter',     it:'Successivo',     pt:'Próximo',      ru:'Следующий',     zh:'下一首', ja:'次へ',    ko:'다음',    ar:'التالي',     he:'הבא',      ur:'اگلا',    hi:'अगला' },
  'music.prev':      { en:'Previous',    es:'Anterior',     fr:'Précédent',  de:'Zurück',     it:'Precedente',     pt:'Anterior',     ru:'Предыдущий',    zh:'上一首', ja:'前へ',    ko:'이전',    ar:'السابق',     he:'הקודם',    ur:'پچھلا',   hi:'पिछला' },

  /* ===== Extrinsics/drill expansion ===== */
  'drill.decodedArgs':  { en:'Decoded Args',    es:'Args. Decodificados', fr:'Args décodés',    de:'Dekodierte Args', it:'Arg. decodificati', pt:'Args decod.',   ru:'Декод. аргументы',  zh:'解码参数', ja:'デコード引数', ko:'디코드 인자', ar:'المعاملات المفكوكة',he:'ארגומנטים מפוענחים',ur:'ڈی کوڈ شدہ آرگز',hi:'डिकोडेड आर्ग्स' },
  'drill.events':       { en:'Events',          es:'Eventos',             fr:'Événements',      de:'Ereignisse',      it:'Eventi',             pt:'Eventos',        ru:'События',           zh:'事件',   ja:'イベント',    ko:'이벤트',  ar:'الأحداث',       he:'אירועים',      ur:'ایونٹس',      hi:'घटनाएँ' },
  'drill.feeBreakdown': { en:'Fee Breakdown',   es:'Desglose de Comisión',fr:'Détail des frais',de:'Gebühren-Aufschl.', it:'Dettaglio commiss.',pt:'Detalhe da taxa',ru:'Разбивка комиссии', zh:'手续费明细',ja:'手数料内訳',ko:'수수료 내역',ar:'تفصيل الرسوم', he:'פירוט עמלה',   ur:'فیس کی تفصیل',hi:'शुल्क विवरण' },
};

/* ========================== Context / provider ========================== */
const LangContext = createContext({ lang: 'es', setLang: () => {}, t: (k) => k, dir: 'ltr', locale: 'es-ES' });

const BCP47 = {
  es: 'es-ES', en: 'en-US', fr: 'fr-FR', de: 'de-DE', it: 'it-IT', pt: 'pt-PT',
  ru: 'ru-RU', zh: 'zh-CN', ja: 'ja-JP', ko: 'ko-KR', ar: 'ar-SA', he: 'he-IL',
  ur: 'ur-PK', hi: 'hi-IN',
};

function LangProvider({ children }) {
  const [lang, setLangState] = useState(() => {
    try {
      const saved = localStorage.getItem('sorametrics.lang');
      if (saved && LANG_BY_CODE[saved]) return saved;
    } catch (_) {}
    return 'es';
  });

  const meta = LANG_BY_CODE[lang] || LANG_BY_CODE.es;
  const dir = meta.rtl ? 'rtl' : 'ltr';

  useEffect(() => {
    document.documentElement.lang = lang;
    document.documentElement.dir = dir;
    document.body.classList.toggle('rtl', dir === 'rtl');
    try { localStorage.setItem('sorametrics.lang', lang); } catch (_) {}
    window.__CURRENT_LANG__ = lang;
  }, [lang, dir]);

  const t = useCallback((key, fallback) => {
    const row = DICT[key];
    if (!row) return fallback !== undefined ? fallback : key;
    return row[lang] || row.en || fallback || key;
  }, [lang]);

  const setLang = useCallback((code) => {
    if (LANG_BY_CODE[code]) setLangState(code);
  }, []);

  const value = useMemo(() => ({
    lang, setLang, t, dir, locale: BCP47[lang] || 'es-ES',
  }), [lang, setLang, t, dir]);

  return <LangContext.Provider value={value}>{children}</LangContext.Provider>;
}

function useLang() { return useContext(LangContext); }
function useT()    { return useContext(LangContext).t; }

/* ========================= Number / date helpers ========================= */
function fmtNumber(n, locale, opts) {
  try { return new Intl.NumberFormat(locale || 'es-ES', opts).format(n); }
  catch (_) { return String(n); }
}
function fmtDate(d, locale, opts) {
  try { return new Intl.DateTimeFormat(locale || 'es-ES', opts || { dateStyle: 'medium', timeStyle: 'short' }).format(d); }
  catch (_) { return String(d); }
}

/* ============================== Lang picker ============================== */
function LangPicker() {
  const { lang, setLang, t } = useLang();
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const meta = LANG_BY_CODE[lang];

  useEffect(() => {
    if (!open) return;
    const onDoc = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    const onEsc = (e) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onDoc);
    document.addEventListener('keydown', onEsc);
    return () => {
      document.removeEventListener('mousedown', onDoc);
      document.removeEventListener('keydown', onEsc);
    };
  }, [open]);

  return (
    <div className="lang-wrap" ref={ref}>
      <button className="lang-pill" onClick={() => setOpen(v => !v)} title={t('common.language')}>
        <span className="lang-flag">{meta.flag}</span>
        <span className="lang-code">{lang.toUpperCase()}</span>
        <span className="lang-caret">▾</span>
      </button>
      {open && (
        <div className="lang-pop" role="menu">
          {LANGS.map(l => (
            <button key={l.code}
                    className={'lang-pop-row' + (l.code === lang ? ' active' : '')}
                    onClick={() => { setLang(l.code); setOpen(false); }}>
              <span className="lang-flag">{l.flag}</span>
              <span className="lang-native">{l.native}</span>
              <span className="lang-pop-code">{l.code.toUpperCase()}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

Object.assign(window, { LANGS, LANG_BY_CODE, LangProvider, useLang, useT, LangPicker, fmtNumber, fmtDate, DICT });
