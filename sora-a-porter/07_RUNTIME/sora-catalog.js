(function () {
  const CATALOG_URL = '../06_DATA/01_catalog_seed_v1.json';

  const ART_TEMPLATES = {

    // ── SORA row ──
    'sora-alive': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <radialGradient id="alive-ign-${rand()}" cx="50%" cy="55%" r="38%">
            <stop offset="0%" stop-color="#FFD58A" stop-opacity="0.95"/>
            <stop offset="55%" stop-color="#E6C275" stop-opacity="0.55"/>
            <stop offset="100%" stop-color="#0A0E1A" stop-opacity="0"/>
          </radialGradient>
        </defs>
        <rect width="200" height="200" fill="#05070D"/>
        <rect width="200" height="200" fill="url(#alive-ign-${rand(true)})"/>
        <g stroke="#C8A85A" stroke-width="0.3" fill="none" opacity="0.7">
          <path d="M0 130 L100 110 L200 130"/>
          <path d="M0 145 L100 125 L200 145"/>
        </g>
        <g stroke="#C8A85A" stroke-width="0.6" fill="none" opacity="0.7">
          <path d="M10 130 Q30 80 50 130 Q70 90 90 130"/>
          <path d="M110 130 Q130 90 150 130 Q170 80 190 130"/>
          <path d="M8 145 Q28 100 48 145 Q68 110 88 145"/>
          <path d="M112 145 Q132 110 152 145 Q172 100 192 145"/>
        </g>
        <circle cx="100" cy="118" r="3" fill="#FFE9B0"/>
        <line x1="100" y1="110" x2="100" y2="135" stroke="#FFE9B0" stroke-width="0.5"/>
        <text x="100" y="175" text-anchor="middle" font-family="EB Garamond, serif" font-size="13" fill="#E6C275" letter-spacing="3.6">SORA</text>
        <text x="100" y="190" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="15" fill="#FFD58A">Alive</text>
      </svg>`,

    'sora-fridays': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <rect width="200" height="200" fill="#1A0F1E"/>
        <g fill="#0A0510">
          <rect x="14" y="100" width="14" height="60"/>
          <rect x="32" y="84" width="12" height="76"/>
          <rect x="48" y="92" width="16" height="68"/>
          <rect x="68" y="76" width="14" height="84"/>
          <rect x="120" y="80" width="14" height="80"/>
          <rect x="138" y="92" width="18" height="68"/>
          <rect x="160" y="88" width="14" height="72"/>
          <rect x="178" y="100" width="14" height="60"/>
        </g>
        <g stroke="#E6C275" stroke-width="0.4" fill="none" opacity="0.55">
          <circle cx="100" cy="92" r="22"/>
          <circle cx="100" cy="92" r="34"/>
          <circle cx="100" cy="92" r="46"/>
        </g>
        <g fill="#1A0F1E">
          <ellipse cx="80" cy="158" rx="6" ry="10"/>
          <ellipse cx="92" cy="160" rx="6" ry="11"/>
          <ellipse cx="105" cy="158" rx="6" ry="10"/>
          <ellipse cx="118" cy="160" rx="6" ry="11"/>
        </g>
        <text x="100" y="100" text-anchor="middle" font-family="EB Garamond, serif" font-size="22" fill="#E6C275" letter-spacing="2">SORA</text>
        <text x="100" y="122" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="18" fill="#FF6E78">Fridays</text>
        <text x="100" y="183" text-anchor="middle" font-family="Inter, sans-serif" font-size="6.5" fill="#BFB6A2" letter-spacing="3">MUSIC · PEOPLE · MOMENTUM</text>
      </svg>`,

    'dawn-is-coming': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <defs><linearGradient id="dawn-sky-${rand()}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#1A2240"/><stop offset="55%" stop-color="#7A4A2A"/><stop offset="100%" stop-color="#FFB870"/></linearGradient></defs>
        <rect width="200" height="200" fill="url(#dawn-sky-${rand(true)})"/>
        <rect x="0" y="138" width="200" height="62" fill="#05070D" opacity="0.85"/>
        <line x1="0" y1="138" x2="200" y2="138" stroke="#FFE9B0" stroke-width="0.6" opacity="0.7"/>
        <ellipse cx="100" cy="140" rx="36" ry="6" fill="#FFE9B0" opacity="0.55"/>
        <circle cx="100" cy="120" r="14" fill="#FFD58A" opacity="0.9"/>
        <g fill="#0A0E1A">
          <path d="M50 138 L60 158 L40 158 Z"/>
          <path d="M140 138 L155 158 L130 158 Z"/>
        </g>
        <text x="100" y="180" text-anchor="middle" font-family="EB Garamond, serif" font-size="11" fill="#E6C275" letter-spacing="3.4">DAWN IS COMING</text>
        <text x="100" y="194" text-anchor="middle" font-family="Inter, sans-serif" font-size="6" fill="#BFB6A2" letter-spacing="2.6">OPEN YOUR EYES</text>
      </svg>`,

    'night-drive': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="nd-sky-${rand()}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#1A0820"/><stop offset="100%" stop-color="#0A0510"/></linearGradient>
          <linearGradient id="nd-road-${rand()}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#0A0510"/><stop offset="100%" stop-color="#1A0820"/></linearGradient>
        </defs>
        <rect width="200" height="200" fill="url(#nd-sky-${rand(true)})"/>
        <rect y="138" width="200" height="62" fill="url(#nd-road-${rand(true)})"/>
        <g stroke="#C13A5C" stroke-width="1" stroke-linecap="round" opacity="0.85">
          <line x1="40" y1="110" x2="80" y2="138"/>
          <line x1="160" y1="110" x2="120" y2="138"/>
        </g>
        <g stroke="#E6C275" stroke-width="0.5" stroke-dasharray="6 6" opacity="0.6">
          <line x1="100" y1="146" x2="100" y2="200"/>
        </g>
        <text x="100" y="86" text-anchor="middle" font-family="EB Garamond, serif" font-size="13" fill="#E6C275" letter-spacing="3.4">NIGHT DRIVE</text>
        <text x="100" y="100" text-anchor="middle" font-family="Inter, sans-serif" font-size="6" fill="#BFB6A2" letter-spacing="2.4">SIDEROADS · 128 BPM</text>
      </svg>`,

    'signal-of-love': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <rect width="200" height="200" fill="#0A0E1A"/>
        <g stroke="#E6C275" stroke-width="0.4" fill="none" opacity="0.6">
          <circle cx="100" cy="96" r="28"/>
          <circle cx="100" cy="96" r="44"/>
          <circle cx="100" cy="96" r="60"/>
          <circle cx="100" cy="96" r="76"/>
        </g>
        <path d="M100 80 C90 70 76 76 80 92 C82 100 100 112 100 112 C100 112 118 100 120 92 C124 76 110 70 100 80 Z" fill="#E6C275" opacity="0.85"/>
        <g fill="#0A0E1A">
          <ellipse cx="78" cy="146" rx="5" ry="9"/>
          <ellipse cx="100" cy="146" rx="5" ry="9"/>
          <ellipse cx="122" cy="146" rx="5" ry="9"/>
        </g>
        <text x="100" y="175" text-anchor="middle" font-family="EB Garamond, serif" font-size="9" fill="#E6C275" letter-spacing="3.2">SIGNAL OF</text>
        <text x="100" y="192" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="16" fill="#FFD58A">Love</text>
      </svg>`,

    'acoustic-sessions': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <rect width="200" height="200" fill="#11172A"/>
        <circle cx="100" cy="90" r="22" fill="#E6C275" opacity="0.85"/>
        <g stroke="#E6C275" stroke-width="0.4" fill="none" opacity="0.55"><circle cx="100" cy="90" r="34"/></g>
        <path d="M0 130 L30 110 L60 130 L90 105 L120 135 L150 115 L180 130 L200 120 L200 200 L0 200 Z" fill="#0A0E1A" stroke="#E6C275" stroke-width="0.5"/>
        <text x="100" y="180" text-anchor="middle" font-family="EB Garamond, serif" font-size="11" fill="#E6C275" letter-spacing="3.2">ACOUSTIC</text>
        <text x="100" y="194" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="14" fill="#FFD58A">sessions</text>
      </svg>`,

    'beats-breath': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <rect width="200" height="200" fill="#0A0E1A"/>
        <g stroke="#E6C275" stroke-width="1.4" stroke-linecap="round">
          <line x1="32" y1="92" x2="32" y2="108"/>
          <line x1="44" y1="84" x2="44" y2="116"/>
          <line x1="56" y1="74" x2="56" y2="126"/>
          <line x1="68" y1="64" x2="68" y2="136"/>
          <line x1="80" y1="56" x2="80" y2="144"/>
          <line x1="92" y1="48" x2="92" y2="152"/>
          <line x1="104" y1="40" x2="104" y2="160"/>
          <line x1="116" y1="48" x2="116" y2="152"/>
          <line x1="128" y1="56" x2="128" y2="144"/>
          <line x1="140" y1="64" x2="140" y2="136"/>
          <line x1="152" y1="74" x2="152" y2="126"/>
          <line x1="164" y1="84" x2="164" y2="116"/>
          <line x1="176" y1="92" x2="176" y2="108"/>
        </g>
        <text x="100" y="180" text-anchor="middle" font-family="EB Garamond, serif" font-size="11" fill="#E6C275" letter-spacing="3.2">BEATS &amp; BREATH</text>
      </svg>`,

    'one': () => `
      <svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
        <rect width="200" height="200" fill="#05070D"/>
        <g stroke="#C8A85A" stroke-width="0.4" fill="none" opacity="0.55">
          <circle cx="100" cy="40" r="30"/>
          <circle cx="100" cy="40" r="48"/>
        </g>
        <rect x="92" y="50" width="16" height="100" fill="#C8A85A"/>
        <rect x="84" y="150" width="32" height="6" fill="#C8A85A"/>
        <text x="100" y="180" text-anchor="middle" font-family="EB Garamond, serif" font-weight="500" font-size="32" fill="#E6C275" letter-spacing="6">ONE</text>
        <text x="100" y="194" text-anchor="middle" font-family="Inter, sans-serif" font-size="6" fill="#BFB6A2" letter-spacing="3">MONUMENT</text>
      </svg>`,

    // ── TONSWAP row · cool blue ──
    'tonswap-pulse': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#06101F"/><g stroke="#4B6FA8" stroke-width="0.3" opacity="0.4"><line x1="0" y1="100" x2="200" y2="100"/></g><path d="M0 100 L40 100 L52 60 L60 140 L72 100 L100 100 L112 70 L120 130 L132 100 L160 100 L172 50 L180 150 L192 100 L200 100" stroke="#9DC0FF" stroke-width="1.4" fill="none" stroke-linejoin="round" stroke-linecap="round"/><text x="100" y="170" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="188" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="16" fill="#E6C275">Pulse</text></svg>`,
    'tonswap-liquidity': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#0A1828"/><path d="M0 140 Q25 124 50 140 T100 140 T150 140 T200 140 L200 200 L0 200 Z" fill="#4B6FA8" opacity="0.55"/><path d="M0 150 Q25 138 50 150 T100 150 T150 150 T200 150" stroke="#9DC0FF" stroke-width="0.6" fill="none" opacity="0.55"/><text x="100" y="60" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="90" text-anchor="middle" font-family="EB Garamond, serif" font-size="20" fill="#E6C275" letter-spacing="2.4">LIQUIDITY</text></svg>`,
    'tonswap-yield': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#06101F"/><g stroke="#4B6FA8" stroke-width="0.4" fill="none" opacity="0.6"><circle cx="100" cy="100" r="52"/><circle cx="100" cy="100" r="38"/></g><path d="M82 90 L100 70 L118 90 M100 70 L100 130" stroke="#E6C275" stroke-width="1.4" fill="none" stroke-linecap="round"/><text x="100" y="170" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="188" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="16" fill="#E6C275">Yield</text></svg>`,
    'tonswap-flow': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#06101F"/><g stroke="#4B6FA8" stroke-width="0.6" fill="none" opacity="0.85"><circle cx="50" cy="80" r="12"/><circle cx="90" cy="100" r="12"/><circle cx="130" cy="80" r="12"/><circle cx="150" cy="120" r="12"/><circle cx="70" cy="130" r="12"/></g><g stroke="#9DC0FF" stroke-width="0.5" opacity="0.7"><path d="M50 80 Q70 60 90 100 Q110 130 130 80 Q150 60 150 120 Q110 140 70 130"/></g><g fill="#9DC0FF" opacity="0.85"><circle cx="50" cy="80" r="1.5"/><circle cx="90" cy="100" r="1.5"/><circle cx="130" cy="80" r="1.5"/><circle cx="150" cy="120" r="1.5"/><circle cx="70" cy="130" r="1.5"/></g><text x="100" y="172" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="190" text-anchor="middle" font-family="EB Garamond, serif" font-size="14" fill="#E6C275" letter-spacing="2">FLOW STATE</text></svg>`,
    'tonswap-nodes': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#06101F"/><g stroke="#4B6FA8" stroke-width="0.5" opacity="0.55"><line x1="40" y1="70" x2="100" y2="110"/><line x1="40" y1="70" x2="80" y2="50"/><line x1="80" y1="50" x2="120" y2="60"/><line x1="120" y1="60" x2="160" y2="80"/><line x1="160" y1="80" x2="150" y2="130"/><line x1="100" y1="110" x2="150" y2="130"/><line x1="100" y1="110" x2="60" y2="140"/><line x1="60" y1="140" x2="110" y2="150"/><line x1="150" y1="130" x2="110" y2="150"/></g><g fill="#9DC0FF"><circle cx="40" cy="70" r="3"/><circle cx="80" cy="50" r="2.6"/><circle cx="120" cy="60" r="3.4"/><circle cx="160" cy="80" r="2.6"/><circle cx="100" cy="110" r="3.8"/><circle cx="60" cy="140" r="2.8"/><circle cx="150" cy="130" r="3"/><circle cx="110" cy="150" r="2.6"/></g><text x="100" y="178" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="194" text-anchor="middle" font-family="EB Garamond, serif" font-size="14" fill="#E6C275" letter-spacing="3">NODES</text></svg>`,
    'tonswap-infinite': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#06101F"/><path d="M50 100 C60 70 90 70 100 100 C110 130 140 130 150 100 C140 70 110 70 100 100 C90 130 60 130 50 100 Z" stroke="#E6C275" stroke-width="1" fill="none"/><path d="M58 100 C66 78 88 78 96 100 C104 122 130 122 138 100 C130 78 108 78 100 100 C92 122 66 122 58 100 Z" stroke="#9DC0FF" stroke-width="0.5" fill="none" opacity="0.6"/><text x="100" y="172" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="188" text-anchor="middle" font-family="EB Garamond, serif" font-size="13" fill="#E6C275" letter-spacing="3.2">INFINITE</text></svg>`,
    'tonswap-vibes': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#06101F"/><g stroke="#4B6FA8" stroke-width="0.5" fill="none" opacity="0.8"><path d="M0 80 Q25 60 50 80 T100 80 T150 80 T200 80"/><path d="M0 100 Q25 80 50 100 T100 100 T150 100 T200 100" stroke="#9DC0FF" stroke-width="0.7"/><path d="M0 120 Q25 100 50 120 T100 120 T150 120 T200 120" opacity="0.7"/><path d="M0 140 Q25 120 50 140 T100 140 T150 140 T200 140" opacity="0.5"/></g><text x="100" y="170" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="190" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="18" fill="#E6C275">vibes</text></svg>`,
    'tonswap-movement': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#06101F"/><g stroke="#4B6FA8" stroke-width="0.5" opacity="0.55"><line x1="20" y1="60" x2="180" y2="60"/><line x1="30" y1="80" x2="170" y2="80"/><line x1="40" y1="100" x2="160" y2="100"/><line x1="50" y1="120" x2="150" y2="120"/></g><g stroke="#9DC0FF" stroke-width="1.4" stroke-linecap="round" opacity="0.95"><line x1="60" y1="60" x2="100" y2="60"/><line x1="60" y1="80" x2="120" y2="80"/><line x1="60" y1="100" x2="140" y2="100"/><line x1="60" y1="120" x2="120" y2="120"/></g><g fill="#E6C275"><polygon points="100,55 110,60 100,65"/><polygon points="120,75 130,80 120,85"/><polygon points="140,95 150,100 140,105"/><polygon points="120,115 130,120 120,125"/></g><text x="100" y="170" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="100" y="190" text-anchor="middle" font-family="EB Garamond, serif" font-size="13" fill="#E6C275" letter-spacing="3">MOVEMENT</text></svg>`,

    // ── SOLSWAP row · warm amber ──
    'solswap-orbit': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#1A0F08"/><circle cx="100" cy="100" r="18" fill="#FFD58A"/><g stroke="#E6A055" stroke-width="0.4" fill="none" opacity="0.7"><ellipse cx="100" cy="100" rx="42" ry="22" transform="rotate(-20 100 100)"/><ellipse cx="100" cy="100" rx="60" ry="34" transform="rotate(15 100 100)"/><ellipse cx="100" cy="100" rx="78" ry="48" transform="rotate(-8 100 100)"/></g><text x="100" y="180" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#E6A055" letter-spacing="3.4">SOLSWAP</text><text x="100" y="194" text-anchor="middle" font-family="EB Garamond, serif" font-size="14" fill="#E6C275" letter-spacing="3">ORBIT</text></svg>`,
    'solswap-liquid-sun': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="ls-sun-${rand()}" cx="50%" cy="60%" r="52%"><stop offset="0%" stop-color="#FFE9B0"/><stop offset="40%" stop-color="#FFB870"/><stop offset="100%" stop-color="#1A0F08" stop-opacity="0"/></radialGradient></defs><rect width="200" height="200" fill="#1A0F08"/><circle cx="100" cy="118" r="58" fill="url(#ls-sun-${rand(true)})"/><g stroke="#FFB870" stroke-width="0.5" opacity="0.55"><line x1="0" y1="150" x2="200" y2="150"/><line x1="20" y1="158" x2="180" y2="158" opacity="0.7"/></g><text x="100" y="56" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#E6A055" letter-spacing="3.4">SOLSWAP</text><text x="100" y="194" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="16" fill="#E6C275">Liquid Sun</text></svg>`,
    'solswap-defi': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#1A0F08"/><g stroke="#E6A055" stroke-width="0.6" fill="none"><polygon points="100,40 158,76 158,138 100,170 42,138 42,76"/><polygon points="100,60 138,82 138,128 100,150 62,128 62,82" opacity="0.7"/><polygon points="100,80 118,90 118,118 100,128 82,118 82,90" opacity="0.5"/></g><text x="100" y="110" text-anchor="middle" font-family="Inter,sans-serif" font-weight="600" font-size="14" fill="#E6C275" letter-spacing="3.2">DEFI</text><text x="100" y="188" text-anchor="middle" font-family="Inter,sans-serif" font-size="8" fill="#E6A055" letter-spacing="3">SOLSWAP</text></svg>`,
    'solswap-solar-wave': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#3A1F10"/><circle cx="100" cy="80" r="22" fill="#FFB870" opacity="0.95"/><g stroke="#FFB870" stroke-width="0.6" fill="none" opacity="0.55"><path d="M0 130 Q40 110 80 130 T160 130 T220 130"/><path d="M0 142 Q40 124 80 142 T160 142 T220 142" opacity="0.75"/></g><path d="M0 168 Q40 156 80 168 T160 168 T220 168 L220 200 L0 200 Z" fill="#E6A055" opacity="0.55"/><text x="100" y="186" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#1A0F08" letter-spacing="3.2">SOLAR WAVE</text><text x="100" y="40" text-anchor="middle" font-family="Inter,sans-serif" font-size="8" fill="#E6A055" letter-spacing="3.4">SOLSWAP</text></svg>`,
    'solswap-eclipse': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="ec-corona-${rand()}" cx="50%" cy="50%" r="42%"><stop offset="0%" stop-color="#FFE9B0" stop-opacity="0.85"/><stop offset="40%" stop-color="#FFB870" stop-opacity="0.45"/><stop offset="100%" stop-color="#05070D" stop-opacity="0"/></radialGradient></defs><rect width="200" height="200" fill="#05070D"/><rect width="200" height="200" fill="url(#ec-corona-${rand(true)})"/><circle cx="100" cy="100" r="44" fill="#FFB870"/><circle cx="108" cy="96" r="42" fill="#05070D"/><circle cx="100" cy="100" r="60" stroke="#E6C275" stroke-width="0.4" fill="none" opacity="0.6"/><text x="100" y="180" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#E6A055" letter-spacing="3.4">SOLSWAP</text><text x="100" y="194" text-anchor="middle" font-family="EB Garamond, serif" font-size="14" fill="#E6C275" letter-spacing="3.2">ECLIPSE</text></svg>`,
    'solswap-community': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#1A0F08"/><g stroke="#E6A055" stroke-width="0.6" fill="none" opacity="0.8"><circle cx="74" cy="92" r="18"/><circle cx="124" cy="86" r="18"/><circle cx="100" cy="120" r="18"/><circle cx="150" cy="124" r="14"/><circle cx="50" cy="124" r="14"/></g><circle cx="100" cy="100" r="9" fill="#FFB870" opacity="0.55"/><text x="100" y="170" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#E6A055" letter-spacing="3.4">SOLSWAP</text><text x="100" y="188" text-anchor="middle" font-family="EB Garamond, serif" font-size="13" fill="#E6C275" letter-spacing="3">COMMUNITY</text></svg>`,
    'solswap-radiate': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="200" height="200" fill="#1A0F08"/><g stroke="#E6A055" stroke-width="0.5" opacity="0.55"><line x1="100" y1="100" x2="100" y2="20"/><line x1="100" y1="100" x2="180" y2="100"/><line x1="100" y1="100" x2="100" y2="180"/><line x1="100" y1="100" x2="20" y2="100"/><line x1="100" y1="100" x2="156" y2="44"/><line x1="100" y1="100" x2="156" y2="156"/><line x1="100" y1="100" x2="44" y2="156"/><line x1="100" y1="100" x2="44" y2="44"/></g><circle cx="100" cy="100" r="14" fill="#FFD58A"/><circle cx="100" cy="100" r="6" fill="#FFE9B0"/><text x="100" y="180" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#E6A055" letter-spacing="3.4">SOLSWAP</text><text x="100" y="194" text-anchor="middle" font-family="EB Garamond, serif" font-size="13" fill="#E6C275" letter-spacing="3">RADIATE</text></svg>`,
    'solswap-sunset': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="sst-sky-${rand()}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#3A1F10"/><stop offset="55%" stop-color="#9C5028"/><stop offset="100%" stop-color="#FFB870"/></linearGradient></defs><rect width="200" height="200" fill="url(#sst-sky-${rand(true)})"/><rect y="140" width="200" height="60" fill="#05070D"/><circle cx="100" cy="140" r="40" fill="#FFD58A" opacity="0.95"/><ellipse cx="100" cy="146" rx="50" ry="6" fill="#FFE9B0" opacity="0.4"/><text x="100" y="184" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#1A0F08" letter-spacing="3.4">SOLSWAP</text><text x="100" y="194" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="11" fill="#1A0F08">Sunset</text></svg>`,

    // ── MEMES row · archive matte ──
    'meme-hodl-the-signal': () => memeTpl({eyebrow:'01', sigil:'antenna', a:'HODL THE', b:'SIGNAL'}),
    'meme-when-signal-hits': () => memeTpl({eyebrow:'02', sigil:'sparks',  a:'WHEN THE', b:'SIGNAL', c:'HITS'}),
    'meme-ngmi-not-here': () => memeTpl({eyebrow:'03', sigil:'staple',  a:'NGMI?', b:'NOT HERE.'}),
    'meme-i-yam-the-node': () => memeTpl({eyebrow:'04', sigil:'yam', a:'I YAM THE', b:'NODE'}),
    'meme-sleep-is-fud': () => memeTpl({eyebrow:'05', sigil:'zzz', a:'SLEEP IS', b:'FUD'}),
    'meme-stack-that-bag': () => memeTpl({eyebrow:'06', sigil:'stack', a:'STACK', b:'THAT BAG'}),
    'meme-decentralize-everything': () => memeTpl({eyebrow:'07', sigil:'network', a:'DECENTRALIZE', b:'EVERYTHING'}),
    'meme-very-early': () => memeTpl({eyebrow:'08', sigil:'compass', a:'VERY', b:'EARLY', swap:true}),

    // ── STATION queue cards ──
    'queue-soras-hill': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="qc-sh-${rand()}" cx="50%" cy="45%" r="40%"><stop offset="0%" stop-color="#FFE9B0" stop-opacity="0.55"/><stop offset="100%" stop-color="#0A0E1A" stop-opacity="0"/></radialGradient></defs><rect width="100" height="100" fill="#0A0E1A"/><rect width="100" height="100" fill="url(#qc-sh-${rand(true)})"/><g stroke="#C8A85A" stroke-width="0.3" fill="none" opacity="0.5"><circle cx="50" cy="48" r="18"/><circle cx="50" cy="48" r="28"/></g><polygon points="50,32 32,74 68,74" fill="#0A0E1A" stroke="#E6C275" stroke-width="0.5"/><polygon points="50,42 38,74 62,74" fill="#1A2240"/><line x1="50" y1="32" x2="50" y2="74" stroke="#FFE9B0" stroke-width="0.4" opacity="0.7"/><circle cx="50" cy="36" r="1.2" fill="#FFE9B0"/></svg>`,
    'queue-xor-sacrifice': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="qc-xor-${rand()}" cx="50%" cy="50%" r="40%"><stop offset="0%" stop-color="#E3242D" stop-opacity="0.45"/><stop offset="100%" stop-color="#1A0810" stop-opacity="0"/></radialGradient></defs><rect width="100" height="100" fill="#1A0810"/><rect width="100" height="100" fill="url(#qc-xor-${rand(true)})"/><g stroke="#E3242D" stroke-width="1.4" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M30 28 L50 48 L70 28"/><path d="M30 72 L50 52 L70 72"/></g><line x1="50" y1="20" x2="50" y2="80" stroke="#FF6E78" stroke-width="0.8" opacity="0.7"/><circle cx="50" cy="50" r="3" fill="#E3242D"/></svg>`,
    'queue-sora-rhapsody': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#0A0E1A"/><g stroke="#C8A85A" stroke-width="0.4" fill="none" opacity="0.8"><ellipse cx="50" cy="50" rx="30" ry="12"/><ellipse cx="50" cy="50" rx="22" ry="9"/><ellipse cx="50" cy="50" rx="14" ry="6"/></g><g stroke="#E6C275" stroke-width="0.5" fill="none" opacity="0.85"><path d="M20 50 Q35 30 50 50 T80 50"/></g><text x="50" y="84" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="9" fill="#E6C275">Rhapsody</text></svg>`,
    'queue-sigil-of-ascent': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#0A0E1A"/><g fill="none" stroke="#E6C275" stroke-width="0.6"><circle cx="50" cy="50" r="28"/><circle cx="50" cy="50" r="20" opacity="0.7"/><polygon points="50,28 70,62 30,62"/><polygon points="50,40 60,57 40,57" opacity="0.7"/></g><circle cx="50" cy="54" r="2" fill="#E6C275"/></svg>`,
    'queue-relic-frequency': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#1A1810"/><g stroke="#BFB6A2" stroke-width="0.4" fill="none" opacity="0.75"><path d="M50 50 m-30 0 a30 14 0 1 0 60 0 a30 14 0 1 0 -60 0"/><path d="M50 50 m-22 0 a22 10 0 1 0 44 0 a22 10 0 1 0 -44 0" opacity="0.7"/></g><circle cx="50" cy="50" r="2" fill="#BFB6A2"/></svg>`,

    // ── STATION signal emissions ──
    'emission-medallion': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#0A0E1A"/><circle cx="50" cy="50" r="36" fill="none" stroke="#E6C275" stroke-width="0.8"/><circle cx="50" cy="50" r="30" fill="none" stroke="#E6C275" stroke-width="0.4" opacity="0.7"/><circle cx="50" cy="50" r="22" fill="#1A2240"/><polygon points="50,34 62,60 38,60" fill="#E6C275"/><line x1="50" y1="32" x2="50" y2="60" stroke="#FFE9B0" stroke-width="0.4" opacity="0.7"/><g stroke="#C8A85A" stroke-width="0.3" opacity="0.7"><line x1="50" y1="14" x2="50" y2="22"/><line x1="50" y1="78" x2="50" y2="86"/><line x1="14" y1="50" x2="22" y2="50"/><line x1="78" y1="50" x2="86" y2="50"/></g><text x="50" y="92" text-anchor="middle" font-family="Inter, sans-serif" font-size="4" fill="#C8A85A" letter-spacing="2">SORA · 7.83 Hz</text></svg>`,
    'emission-capsule': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#1A1810"/><rect x="10" y="10" width="80" height="80" fill="none" stroke="#BFB6A2" stroke-width="0.4" stroke-dasharray="2 2" opacity="0.7"/><rect x="38" y="20" width="24" height="62" rx="12" fill="#0A0E1A" stroke="#BFB6A2" stroke-width="0.6"/><rect x="38" y="20" width="24" height="32" rx="12" fill="#BFB6A2" opacity="0.18"/><text x="50" y="44" text-anchor="middle" font-family="JetBrains Mono, monospace" font-size="5" fill="#BFB6A2">07.83</text><text x="50" y="68" text-anchor="middle" font-family="Inter, sans-serif" font-size="3.5" fill="#BFB6A2" letter-spacing="1">CAPSULE</text></svg>`,
    'emission-poster': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#05070D"/><rect x="18" y="10" width="64" height="80" fill="#1A2240" stroke="#E6C275" stroke-width="0.6"/><text x="50" y="26" text-anchor="middle" font-family="EB Garamond, serif" font-size="9" fill="#E6C275" letter-spacing="2">SORA</text><g stroke="#C8A85A" stroke-width="0.3" fill="none" opacity="0.5"><circle cx="50" cy="56" r="12"/><circle cx="50" cy="56" r="18"/></g><polygon points="50,48 56,62 44,62" fill="#E6C275"/><text x="50" y="80" text-anchor="middle" font-family="Inter, sans-serif" font-size="3.5" fill="#BFB6A2" letter-spacing="1.5">FROM ANYWHERE</text></svg>`,
    'emission-key': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#0A0E1A"/><circle cx="32" cy="32" r="16" fill="none" stroke="#E6C275" stroke-width="1"/><circle cx="32" cy="32" r="10" fill="none" stroke="#E6C275" stroke-width="0.5" opacity="0.7"/><polygon points="26,32 32,24 38,32 32,40" fill="none" stroke="#E6C275" stroke-width="0.5" opacity="0.7"/><line x1="44" y1="44" x2="84" y2="84" stroke="#E6C275" stroke-width="2" stroke-linecap="round"/><rect x="68" y="68" width="6" height="3" fill="#E6C275" transform="rotate(45 71 70)"/><rect x="74" y="74" width="6" height="3" fill="#E6C275" transform="rotate(45 77 76)"/><text x="50" y="96" text-anchor="middle" font-family="JetBrains Mono, monospace" font-size="4" fill="#C8A85A" letter-spacing="2">KEY · 01</text></svg>`,
    'emission-vinyl': () => `<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100" fill="#1A0810"/><circle cx="50" cy="50" r="40" fill="#0A0510"/><g stroke="#3A0A14" stroke-width="0.3" fill="none" opacity="0.85"><circle cx="50" cy="50" r="36"/><circle cx="50" cy="50" r="32"/><circle cx="50" cy="50" r="28"/><circle cx="50" cy="50" r="24"/><circle cx="50" cy="50" r="20"/></g><circle cx="50" cy="50" r="12" fill="#E3242D"/><text x="50" y="48" text-anchor="middle" font-family="EB Garamond, serif" font-size="6" fill="#FFE9B0">XOR</text><text x="50" y="55" text-anchor="middle" font-family="Inter, sans-serif" font-size="3.4" fill="#FFE9B0" letter-spacing="1">SACRIFICE</text><circle cx="50" cy="50" r="1.4" fill="#0A0510"/></svg>`,

    // ── POSTERS · portrait-oriented poster art ──
    'poster-sorametrics-tesla': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="pst-${rand()}" cx="50%" cy="40%" r="65%"><stop offset="0%" stop-color="#1A2240"/><stop offset="55%" stop-color="#080810"/><stop offset="100%" stop-color="#020207"/></radialGradient><radialGradient id="pst-halo-${rand()}" cx="50%" cy="38%" r="38%"><stop offset="0%" stop-color="#D4A85A" stop-opacity="0.5"/><stop offset="100%" stop-color="#D4A85A" stop-opacity="0"/></radialGradient><linearGradient id="pst-bolt-${rand()}" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#9DC0FF" stop-opacity="0.85"/><stop offset="100%" stop-color="#3D5E94" stop-opacity="0.25"/></linearGradient></defs><rect width="400" height="560" fill="url(#pst-${rand(true)})"/><rect width="400" height="560" fill="url(#pst-halo-${rand(true)})"/><g stroke="#B08C44" stroke-width="0.4" fill="none" opacity="0.4"><circle cx="200" cy="220" r="60"/><circle cx="200" cy="220" r="100"/><circle cx="200" cy="220" r="140"/><circle cx="200" cy="220" r="180"/></g><g stroke="url(#pst-bolt-${rand(true)})" stroke-width="1.2" fill="none" stroke-linecap="round"><path d="M70 100 L130 140 L110 155 L170 180"/><path d="M330 100 L270 140 L290 155 L230 180"/><path d="M60 240 L120 258 L100 270 L160 290"/><path d="M340 240 L280 258 L300 270 L240 290"/></g><g transform="translate(200 220)"><ellipse cx="0" cy="-20" rx="32" ry="40" fill="#0A0E1A" stroke="#B08C44" stroke-width="0.4"/><path d="M-22 -42 Q0 -68 22 -42" fill="#1A2240"/><circle cx="-10" cy="-22" r="2" fill="#B08C44"/><circle cx="10" cy="-22" r="2" fill="#B08C44"/></g><text x="200" y="380" text-anchor="middle" font-family="EB Garamond, serif" font-size="42" fill="#D4A85A" letter-spacing="2">SORAMETRICS</text><text x="200" y="410" text-anchor="middle" font-family="Inter, sans-serif" font-size="10" fill="#948B72" letter-spacing="3.4">SIGNAL · RESONANCE · CONNECTION</text><text x="200" y="500" text-anchor="middle" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="3">FROM ANYWHERE · TO EVERYWHERE</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#B08C44" stroke-width="0.6" opacity="0.55"/></svg>`,

    'poster-frequency-shortwave': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="400" height="560" fill="#1A1810"/><g stroke="#B08C44" stroke-width="0.3" fill="none" opacity="0.6"><circle cx="200" cy="240" r="50"/><circle cx="200" cy="240" r="100"/><circle cx="200" cy="240" r="150"/></g><line x1="200" y1="120" x2="200" y2="360" stroke="#B08C44" stroke-width="2"/><rect x="180" y="115" width="40" height="6" fill="#B08C44"/><rect x="185" y="360" width="30" height="10" fill="#B08C44"/><g fill="#3D5E94" opacity="0.6"><circle cx="100" cy="180" r="3"/><circle cx="300" cy="180" r="3"/></g><g fill="#9C5028" opacity="0.7"><circle cx="80" cy="280" r="3"/><circle cx="320" cy="280" r="3"/></g><text x="200" y="60" text-anchor="middle" font-family="Inter, sans-serif" font-size="9" fill="#948B72" letter-spacing="4">SIGNAL FOR DREAMERS, ENGINEERS, LOVERS, STRANGERS</text><text x="200" y="430" text-anchor="middle" font-family="EB Garamond, serif" font-size="36" fill="#D4A85A" letter-spacing="3">FREQUENCY</text><text x="200" y="460" text-anchor="middle" font-family="Inter, sans-serif" font-size="10" fill="#948B72" letter-spacing="3">MULTI-GENRE · WORLD BAND</text><text x="200" y="520" text-anchor="middle" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="3">FROM ANYWHERE · TO EVERYWHERE</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#B08C44" stroke-width="0.6" opacity="0.55"/></svg>`,

    'poster-electric-sabbath': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="es-bg-${rand()}" cx="50%" cy="50%" r="60%"><stop offset="0%" stop-color="#2A2A30"/><stop offset="100%" stop-color="#08080F"/></radialGradient></defs><rect width="400" height="560" fill="url(#es-bg-${rand(true)})"/><g stroke="#9DC0FF" stroke-width="1" stroke-linecap="round" opacity="0.7"><line x1="80" y1="120" x2="120" y2="200"/><line x1="320" y1="120" x2="280" y2="200"/></g><rect x="180" y="190" width="40" height="120" fill="none" stroke="#B08C44" stroke-width="0.6"/><circle cx="200" cy="220" r="14" fill="#D4A85A"/><line x1="200" y1="234" x2="200" y2="290" stroke="#D4A85A" stroke-width="1.5"/><text x="200" y="110" text-anchor="middle" font-family="EB Garamond, serif" font-size="36" fill="#D4A85A" letter-spacing="2.4">ELECTRIC</text><text x="200" y="148" text-anchor="middle" font-family="EB Garamond, serif" font-size="36" fill="#D4A85A" letter-spacing="2.4">SABBATH</text><text x="200" y="356" text-anchor="middle" font-family="Inter, sans-serif" font-size="9" fill="#948B72" letter-spacing="3">INSTRUMENTAL · F MINOR · 132 BPM</text><g stroke="#B08C44" stroke-width="0.3" opacity="0.4"><line x1="80" y1="400" x2="320" y2="400"/></g><text x="80" y="430" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">1. IMPACT</text><text x="80" y="450" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">2. PULSE</text><text x="80" y="470" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">3. SPACE</text><text x="80" y="490" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">4. LAMENT</text><text x="240" y="430" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">5. BREATHE</text><text x="240" y="450" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">6. RETURN</text><text x="240" y="470" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">7. ASCENT</text><text x="240" y="490" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="2">8. SURGE</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#B08C44" stroke-width="0.6" opacity="0.55"/></svg>`,

    'poster-world-band': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="400" height="560" fill="#3A1F10"/><g stroke="#D4A85A" stroke-width="0.4" fill="none" opacity="0.7"><circle cx="200" cy="240" r="40"/><circle cx="200" cy="240" r="80"/><circle cx="200" cy="240" r="120"/><circle cx="200" cy="240" r="160"/></g><line x1="200" y1="160" x2="200" y2="320" stroke="#D4A85A" stroke-width="2"/><rect x="190" y="155" width="20" height="6" fill="#D4A85A"/><rect x="184" y="320" width="32" height="12" fill="#D4A85A"/><rect x="180" y="332" width="40" height="20" fill="#D4A85A" opacity="0.55"/><text x="200" y="50" text-anchor="middle" font-family="Inter, sans-serif" font-size="10" fill="#FFE9B0" letter-spacing="3.4">SIGNAL FOR DREAMERS, ENGINEERS, LOVERS, STRANGERS</text><text x="200" y="400" text-anchor="middle" font-family="EB Garamond, serif" font-size="34" fill="#FFE9B0" letter-spacing="3.4">SORAMETRICS</text><text x="200" y="425" text-anchor="middle" font-family="EB Garamond, serif" font-size="22" fill="#FFE9B0" letter-spacing="2">RADIO STATION</text><text x="200" y="460" text-anchor="middle" font-family="Inter, sans-serif" font-size="9" fill="#E6A055" letter-spacing="3.4">INTERNATIONAL SHORTWAVE SERVICE</text><text x="200" y="478" text-anchor="middle" font-family="Inter, sans-serif" font-size="13" fill="#FFE9B0" letter-spacing="4">WORLD BAND · 49 m</text><text x="80" y="520" font-family="Inter, sans-serif" font-size="7" fill="#E6A055" letter-spacing="2">FROM ANYWHERE</text><text x="320" y="520" text-anchor="end" font-family="Inter, sans-serif" font-size="7" fill="#E6A055" letter-spacing="2">TO EVERYWHERE</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#FFE9B0" stroke-width="0.6" opacity="0.55"/></svg>`,

    'poster-sora-alive': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="sa-ign-${rand()}" cx="50%" cy="42%" r="38%"><stop offset="0%" stop-color="#FFD58A" stop-opacity="0.95"/><stop offset="55%" stop-color="#D4A85A" stop-opacity="0.55"/><stop offset="100%" stop-color="#020207" stop-opacity="0"/></radialGradient></defs><rect width="400" height="560" fill="#020207"/><rect width="400" height="560" fill="url(#sa-ign-${rand(true)})"/><g stroke="#B08C44" stroke-width="0.4" fill="none" opacity="0.7"><path d="M40 320 L200 250 L360 320"/><path d="M50 340 L200 270 L350 340"/><path d="M60 360 L200 290 L340 360"/></g><g stroke="#B08C44" stroke-width="0.8" fill="none" opacity="0.65"><path d="M30 320 Q50 240 70 320 Q90 250 110 320"/><path d="M290 320 Q310 250 330 320 Q350 240 370 320"/></g><circle cx="200" cy="240" r="4" fill="#FFE9B0"/><line x1="200" y1="230" x2="200" y2="260" stroke="#FFE9B0" stroke-width="0.5"/><text x="200" y="430" text-anchor="middle" font-family="EB Garamond, serif" font-size="48" fill="#FFE9B0" letter-spacing="4">SORA</text><text x="200" y="478" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="46" fill="#FFD58A">Alive</text><text x="200" y="516" text-anchor="middle" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="3">DAWN IS COMING · OPEN YOUR EYES</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#B08C44" stroke-width="0.6" opacity="0.55"/></svg>`,

    'poster-nexus-frequency': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="nx-bg-${rand()}" cx="50%" cy="40%" r="60%"><stop offset="0%" stop-color="#3B0F2A"/><stop offset="55%" stop-color="#1A0F1E"/><stop offset="100%" stop-color="#0A0510"/></radialGradient></defs><rect width="400" height="560" fill="url(#nx-bg-${rand(true)})"/><g stroke="#D4A85A" stroke-width="0.4" fill="none" opacity="0.55"><circle cx="200" cy="220" r="50"/><circle cx="200" cy="220" r="90"/><circle cx="200" cy="220" r="130"/></g><circle cx="200" cy="220" r="14" fill="#FFD58A"/><line x1="200" y1="234" x2="200" y2="320" stroke="#FFD58A" stroke-width="0.6"/><g fill="#1A0F1E"><ellipse cx="160" cy="370" rx="10" ry="20"/><ellipse cx="180" cy="372" rx="10" ry="22"/><ellipse cx="200" cy="370" rx="10" ry="20"/><ellipse cx="220" cy="372" rx="10" ry="22"/><ellipse cx="240" cy="370" rx="10" ry="20"/></g><text x="200" y="430" text-anchor="middle" font-family="EB Garamond, serif" font-size="34" fill="#D4A85A" letter-spacing="4">NEXUS</text><text x="200" y="468" text-anchor="middle" font-family="EB Garamond, serif" font-size="28" fill="#FF6E78" letter-spacing="3">FREQUENCY</text><text x="200" y="512" text-anchor="middle" font-family="Inter, sans-serif" font-size="8" fill="#948B72" letter-spacing="3">SORAMETRICS.ORG</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#D4A85A" stroke-width="0.6" opacity="0.55"/></svg>`,

    'poster-undone': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="un-bg-${rand()}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#1A1A2A"/><stop offset="100%" stop-color="#080810"/></linearGradient></defs><rect width="400" height="560" fill="url(#un-bg-${rand(true)})"/><g stroke="#948B72" stroke-width="0.4" opacity="0.4"><line x1="60" y1="180" x2="80" y2="200"/><line x1="80" y1="200" x2="100" y2="195"/><line x1="60" y1="220" x2="90" y2="240"/><line x1="320" y1="180" x2="340" y2="200"/><line x1="340" y1="200" x2="320" y2="195"/></g><g fill="#9DC0FF" opacity="0.3"><rect x="160" y="160" width="80" height="180"/></g><circle cx="200" cy="200" r="36" fill="#EFE5CC" opacity="0.85"/><circle cx="200" cy="220" r="30" fill="#EFE5CC" opacity="0.7"/><g stroke="#E3242D" stroke-width="0.6" fill="none" opacity="0.7"><line x1="330" y1="160" x2="345" y2="190"/></g><circle cx="345" cy="190" r="3" fill="#E3242D"/><text x="200" y="460" text-anchor="middle" font-family="EB Garamond, serif" font-size="44" fill="#EFE5CC" letter-spacing="2">Undone</text><text x="200" y="500" text-anchor="middle" font-family="Inter, sans-serif" font-size="12" fill="#948B72" letter-spacing="4.4">NEVER YOURS</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#B08C44" stroke-width="0.6" opacity="0.55"/></svg>`,

    'poster-sora-fridays': () => `<svg viewBox="0 0 400 560" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="sf-sky-${rand()}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#3B0F2A"/><stop offset="55%" stop-color="#1A0F1E"/><stop offset="100%" stop-color="#0A0510"/></linearGradient><radialGradient id="sf-pop-${rand()}" cx="50%" cy="50%" r="40%"><stop offset="0%" stop-color="#D4A85A" stop-opacity="0.65"/><stop offset="100%" stop-color="#C13A5C" stop-opacity="0"/></radialGradient></defs><rect width="400" height="560" fill="url(#sf-sky-${rand(true)})"/><rect width="400" height="560" fill="url(#sf-pop-${rand(true)})"/><g fill="#0A0510"><rect x="40" y="220" width="24" height="120"/><rect x="68" y="200" width="22" height="140"/><rect x="94" y="210" width="28" height="130"/><rect x="126" y="180" width="22" height="160"/><rect x="240" y="190" width="22" height="150"/><rect x="266" y="210" width="30" height="130"/><rect x="300" y="200" width="22" height="140"/><rect x="330" y="220" width="24" height="120"/></g><g stroke="#D4A85A" stroke-width="0.4" fill="none" opacity="0.55"><circle cx="200" cy="220" r="50"/><circle cx="200" cy="220" r="80"/></g><g fill="#1A0F1E"><ellipse cx="160" cy="380" rx="10" ry="20"/><ellipse cx="180" cy="382" rx="10" ry="22"/><ellipse cx="200" cy="380" rx="10" ry="20"/><ellipse cx="220" cy="382" rx="10" ry="22"/><ellipse cx="240" cy="380" rx="10" ry="20"/></g><text x="200" y="240" text-anchor="middle" font-family="EB Garamond, serif" font-size="40" fill="#D4A85A" letter-spacing="2.5">SORA</text><text x="200" y="290" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="36" fill="#FF6E78">Fridays</text><text x="200" y="450" text-anchor="middle" font-family="Inter, sans-serif" font-size="9" fill="#948B72" letter-spacing="3.4">MUSIC · PEOPLE · MOMENTUM</text><rect x="14" y="14" width="372" height="532" fill="none" stroke="#D4A85A" stroke-width="0.6" opacity="0.55"/></svg>`,

    // ── COLLECTIONS · capsule covers ──
    'collection-sora':    () => `<svg viewBox="0 0 320 220" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="cs-${rand()}" cx="50%" cy="50%" r="55%"><stop offset="0%" stop-color="#FFD58A" stop-opacity="0.45"/><stop offset="100%" stop-color="#020207" stop-opacity="0"/></radialGradient></defs><rect width="320" height="220" fill="#020207"/><rect width="320" height="220" fill="url(#cs-${rand(true)})"/><g stroke="#B08C44" stroke-width="0.4" fill="none" opacity="0.6"><circle cx="160" cy="110" r="40"/><circle cx="160" cy="110" r="60"/><circle cx="160" cy="110" r="80"/></g><circle cx="160" cy="110" r="6" fill="#D4A85A"/><text x="160" y="180" text-anchor="middle" font-family="EB Garamond, serif" font-size="22" fill="#D4A85A" letter-spacing="3">SORA</text><text x="160" y="200" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#948B72" letter-spacing="3">ORIGINALS · 08</text></svg>`,
    'collection-tonswap': () => `<svg viewBox="0 0 320 220" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="320" height="220" fill="#06101F"/><path d="M0 140 Q40 124 80 140 T160 140 T240 140 T320 140 L320 220 L0 220 Z" fill="#3D5E94" opacity="0.55"/><g stroke="#9DC0FF" stroke-width="0.5" fill="none" opacity="0.6"><circle cx="100" cy="90" r="3"/><circle cx="160" cy="78" r="3"/><circle cx="220" cy="92" r="3"/></g><text x="160" y="60" text-anchor="middle" font-family="Inter, sans-serif" font-size="11" fill="#9DC0FF" letter-spacing="3.4">TONSWAP</text><text x="160" y="200" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#948B72" letter-spacing="3">PARTNER ECOSYSTEM · 08</text></svg>`,
    'collection-solswap': () => `<svg viewBox="0 0 320 220" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="320" height="220" fill="#1A0F08"/><defs><radialGradient id="csl-${rand()}" cx="50%" cy="55%" r="50%"><stop offset="0%" stop-color="#FFD58A"/><stop offset="40%" stop-color="#FFB870"/><stop offset="100%" stop-color="#1A0F08" stop-opacity="0"/></radialGradient></defs><circle cx="160" cy="120" r="58" fill="url(#csl-${rand(true)})"/><g stroke="#E6A055" stroke-width="0.4" opacity="0.55"><line x1="60" y1="170" x2="260" y2="170"/></g><text x="160" y="60" text-anchor="middle" font-family="Inter, sans-serif" font-size="11" fill="#E6A055" letter-spacing="3.4">SOLSWAP</text><text x="160" y="200" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#948B72" letter-spacing="3">PARTNER ECOSYSTEM · 08</text></svg>`,
    'collection-memes':   () => `<svg viewBox="0 0 320 220" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="320" height="220" fill="#1A1810"/><rect x="14" y="14" width="292" height="192" fill="none" stroke="#948B72" stroke-width="0.4" stroke-dasharray="3 2"/><text x="160" y="100" text-anchor="middle" font-family="Inter, sans-serif" font-size="22" font-weight="800" fill="#948B72">ARCHIVE</text><text x="160" y="138" text-anchor="middle" font-family="Inter, sans-serif" font-size="28" font-weight="800" fill="#D4A85A">RELIC</text><text x="160" y="180" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#948B72" letter-spacing="3">MEMES · 08 CAPSULES</text></svg>`,
    'collection-tesla':   () => `<svg viewBox="0 0 320 220" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="320" height="220" fill="#080810"/><g stroke="#9DC0FF" stroke-width="0.6" fill="none" opacity="0.7" stroke-linecap="round"><path d="M60 50 L100 80 L85 92 L120 110"/><path d="M260 50 L220 80 L235 92 L200 110"/></g><g stroke="#B08C44" stroke-width="0.4" fill="none" opacity="0.55"><circle cx="160" cy="110" r="40"/><circle cx="160" cy="110" r="70"/></g><ellipse cx="160" cy="95" rx="20" ry="26" fill="#0A0E1A" stroke="#B08C44" stroke-width="0.4"/><text x="160" y="200" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#948B72" letter-spacing="3">TESLA SERIES · 04</text></svg>`,
    'collection-founder': () => `<svg viewBox="0 0 320 220" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><rect width="320" height="220" fill="#020207"/><g stroke="#D4A85A" stroke-width="0.5" fill="none" opacity="0.8"><circle cx="160" cy="110" r="44"/><circle cx="160" cy="110" r="32"/></g><polygon points="160,80 175,130 145,130" fill="#D4A85A" opacity="0.85"/><text x="160" y="190" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="14" fill="#D4A85A">Founder Edits</text><text x="160" y="208" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#948B72" letter-spacing="3">06 LIMITED CAPSULES</text></svg>`,

    // ── JOURNAL dispatch covers ──
    'dispatch-tweetstorm': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><radialGradient id="ep2-bg-${rand()}" cx="50%" cy="40%" r="60%"><stop offset="0%" stop-color="#1A2240"/><stop offset="55%" stop-color="#0A0E1A"/><stop offset="100%" stop-color="#05070D"/></radialGradient><radialGradient id="ep2-halo-${rand()}" cx="50%" cy="38%" r="32%"><stop offset="0%" stop-color="#E6C275" stop-opacity="0.5"/><stop offset="100%" stop-color="#E6C275" stop-opacity="0"/></radialGradient></defs><rect width="200" height="200" fill="url(#ep2-bg-${rand(true)})"/><rect width="200" height="200" fill="url(#ep2-halo-${rand(true)})"/><g stroke="#C8A85A" stroke-width="0.3" fill="none" opacity="0.5"><circle cx="100" cy="80" r="32"/><circle cx="100" cy="80" r="48"/><circle cx="100" cy="80" r="64"/></g><g stroke="#9DC0FF" stroke-width="0.5" fill="none" opacity="0.7"><path d="M50 60 L70 70 L60 78 L84 88"/><path d="M150 60 L130 70 L140 78 L116 88"/></g><text x="100" y="135" text-anchor="middle" font-family="EB Garamond, serif" font-size="11" fill="#E6C275" letter-spacing="2.2">DISPATCH</text><text x="100" y="155" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="22" fill="#FFE9B0">Tweetstorm</text><text x="100" y="178" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#BFB6A2" letter-spacing="2.6">SORA · MAY 2026 · 06:16</text></svg>`,
    'dispatch-nexus': () => `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="ep1-sky-${rand()}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#3A1F10"/><stop offset="55%" stop-color="#1A0F08"/><stop offset="100%" stop-color="#0A0510"/></linearGradient><radialGradient id="ep1-glow-${rand()}" cx="50%" cy="50%" r="44%"><stop offset="0%" stop-color="#FFB870" stop-opacity="0.45"/><stop offset="100%" stop-color="#1A0F08" stop-opacity="0"/></radialGradient></defs><rect width="200" height="200" fill="url(#ep1-sky-${rand(true)})"/><rect width="200" height="200" fill="url(#ep1-glow-${rand(true)})"/><g stroke="#E6A055" stroke-width="0.4" fill="none" opacity="0.6"><circle cx="100" cy="100" r="40"/><circle cx="100" cy="100" r="56"/><circle cx="100" cy="100" r="72"/></g><polygon points="100,72 84,116 116,116" fill="none" stroke="#E6C275" stroke-width="0.7"/><text x="100" y="148" text-anchor="middle" font-family="EB Garamond, serif" font-size="11" fill="#E6C275" letter-spacing="2.2">FOUNDING</text><text x="100" y="170" text-anchor="middle" font-family="EB Garamond, serif" font-style="italic" font-size="20" fill="#FFE9B0">Nexus</text><text x="100" y="190" text-anchor="middle" font-family="Inter, sans-serif" font-size="7" fill="#BFB6A2" letter-spacing="2.6">SORA · MAY 2026 · 04:55</text></svg>`,

  };

  let _randCounter = 0;
  let _randCache = 0;
  function rand(reuse) {
    if (reuse) return _randCache;
    _randCounter += 1;
    _randCache = _randCounter;
    return _randCounter;
  }

  // memes shared template
  function memeSigil(kind) {
    switch (kind) {
      case 'antenna': return '<g stroke="#E6C275" stroke-width="0.5" fill="none" opacity="0.55"><path d="M85 38 Q100 22 115 38"/><line x1="100" y1="22" x2="100" y2="40"/><circle cx="100" cy="38" r="1.4" fill="#E6C275"/></g>';
      case 'sparks':  return '<g stroke="#E6C275" stroke-width="1" stroke-linecap="round" opacity="0.65"><line x1="100" y1="28" x2="100" y2="36"/><line x1="84" y1="32" x2="90" y2="38"/><line x1="116" y1="32" x2="110" y2="38"/><line x1="74" y1="42" x2="82" y2="46"/><line x1="126" y1="42" x2="118" y2="46"/></g>';
      case 'staple':  return '<g stroke="#BFB6A2" stroke-width="0.4" opacity="0.5"><line x1="60" y1="44" x2="140" y2="44"/><line x1="60" y1="44" x2="60" y2="56"/><line x1="140" y1="44" x2="140" y2="56"/></g>';
      case 'yam':     return '<g stroke="#BFB6A2" stroke-width="0.4" opacity="0.55"><circle cx="100" cy="38" r="6"/><line x1="100" y1="44" x2="100" y2="52"/><line x1="86" y1="38" x2="80" y2="32"/><line x1="114" y1="38" x2="120" y2="32"/></g>';
      case 'zzz':     return '<g font-family="EB Garamond, serif" font-style="italic" fill="#BFB6A2" opacity="0.55" text-anchor="middle"><text x="138" y="36" font-size="14">z</text><text x="152" y="48" font-size="18">z</text><text x="168" y="64" font-size="22">Z</text></g>';
      case 'stack':   return '<g stroke="#BFB6A2" stroke-width="0.4" fill="none" opacity="0.55"><rect x="86" y="36" width="28" height="10"/><rect x="80" y="46" width="40" height="10"/><rect x="74" y="56" width="52" height="10"/></g>';
      case 'network': return '<g stroke="#BFB6A2" stroke-width="0.4" opacity="0.55"><circle cx="100" cy="40" r="6" fill="none"/><line x1="100" y1="40" x2="86" y2="34"/><line x1="100" y1="40" x2="114" y2="34"/><line x1="100" y1="40" x2="80" y2="48"/><line x1="100" y1="40" x2="120" y2="48"/><circle cx="86" cy="34" r="2"/><circle cx="114" cy="34" r="2"/><circle cx="80" cy="48" r="2"/><circle cx="120" cy="48" r="2"/></g>';
      case 'compass': return '<g stroke="#E6C275" stroke-width="0.4" fill="none" opacity="0.55"><circle cx="100" cy="38" r="6"/><line x1="100" y1="44" x2="100" y2="52"/><line x1="92" y1="46" x2="98" y2="50"/><line x1="108" y1="46" x2="102" y2="50"/></g>';
      default: return '';
    }
  }

  function memeTpl(opts) {
    const { eyebrow, sigil, a, b, c, swap } = opts;
    const colA = swap ? '#E6C275' : '#BFB6A2';
    const colB = swap ? '#BFB6A2' : '#E6C275';
    const lineA = `<text x="100" y="100" text-anchor="middle" font-family="Inter,sans-serif" font-size="${swap ? 30 : 14}" font-weight="800" fill="${colA}" letter-spacing="${swap ? 3 : 1.4}">${a}</text>`;
    const lineB = `<text x="100" y="${swap ? 136 : c ? 124 : 128}" text-anchor="middle" font-family="Inter,sans-serif" font-size="${swap ? 30 : c ? 22 : 24}" font-weight="800" fill="${colB}" letter-spacing="${swap ? 3 : 1.6}">${b}</text>`;
    const lineC = c ? `<text x="100" y="150" text-anchor="middle" font-family="Inter,sans-serif" font-size="14" font-weight="800" fill="${colA}" letter-spacing="1.8">${c}</text>` : '';
    return `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice" aria-hidden="true" xmlns="http://www.w3.org/2000/svg">
      <rect width="200" height="200" fill="#1A1810"/>
      <rect x="10" y="10" width="180" height="180" fill="none" stroke="#BFB6A2" stroke-width="0.4" stroke-dasharray="3 2"/>
      ${memeSigil(sigil)}
      ${lineA}
      ${lineB}
      ${lineC}
      <text x="100" y="${c ? 175 : 160}" text-anchor="middle" font-family="Inter,sans-serif" font-size="7" fill="#BFB6A2" letter-spacing="3">ARCHIVE RELIC · ${eyebrow}</text>
      <text x="22" y="186" font-family="JetBrains Mono, monospace" font-size="6" fill="#BFB6A2" opacity="0.7">N° ${eyebrow} / 999</text>
    </svg>`;
  }

  function svgFrom(html) {
    const parsed = new DOMParser().parseFromString(html, 'image/svg+xml');
    return parsed.documentElement;
  }

  function renderRowCard(item, opts) {
    const showEdition = opts && opts.showEdition && item.edition;
    const a = document.createElement('a');
    a.className = 'card' + (showEdition ? ' card--detailed' : '');
    a.href = item.href || '#';
    const art = document.createElement('div');
    art.className = 'card__art';
    if (item.archive || (item.family || '').includes('Archive')) art.dataset.surface = 'archive';
    const svgHtml = ART_TEMPLATES[item.art] ? ART_TEMPLATES[item.art]() : '';
    if (svgHtml) art.appendChild(svgFrom(svgHtml));
    a.appendChild(art);
    const title = document.createElement('p');
    title.className = 'card__title';
    title.textContent = item.title;
    a.appendChild(title);
    const meta = document.createElement('p');
    meta.className = 'card__meta';
    meta.textContent = item.family;
    a.appendChild(meta);
    if (showEdition) {
      const ed = document.createElement('p');
      ed.className = 'card__edition';
      ed.textContent = 'N° ' + item.edition;
      a.appendChild(ed);
    }
    return a;
  }

  function renderQueueCard(item, index) {
    const btn = document.createElement('button');
    btn.className = 'queue-card';
    btn.type = 'button';
    if (item.status) btn.dataset.status = item.status;
    btn.dataset.track = item.slug;
    btn.dataset.title = item.title;
    btn.dataset.family = item.family;
    btn.dataset.dur = item.duration;
    if (item.src) btn.dataset.src = item.src;

    const idx = document.createElement('span');
    idx.className = 'queue-card__index';
    idx.textContent = String(index + 1).padStart(2, '0');
    btn.appendChild(idx);

    const art = document.createElement('div');
    art.className = 'queue-card__art';
    if (item.archive) art.dataset.surface = 'archive';
    const svgHtml = ART_TEMPLATES[item.art] ? ART_TEMPLATES[item.art]() : '';
    if (svgHtml) art.appendChild(svgFrom(svgHtml));
    btn.appendChild(art);

    const t = document.createElement('span');
    t.className = 'queue-card__title';
    t.textContent = formatTitle(item.title);
    btn.appendChild(t);

    const f = document.createElement('span');
    f.className = 'queue-card__family';
    f.textContent = item.family;
    btn.appendChild(f);

    const d = document.createElement('span');
    d.className = 'queue-card__dur';
    d.textContent = item.duration;
    btn.appendChild(d);

    const s = document.createElement('span');
    s.className = 'queue-card__status';
    s.textContent = item.label || String(index + 1);
    btn.appendChild(s);

    return btn;
  }

  function renderEmission(item) {
    const a = document.createElement('a');
    a.className = 'emission';
    a.href = item.href || '#';

    const art = document.createElement('div');
    art.className = 'emission__art';
    if (item.archive) art.dataset.surface = 'archive';
    const svgHtml = ART_TEMPLATES[item.art] ? ART_TEMPLATES[item.art]() : '';
    if (svgHtml) art.appendChild(svgFrom(svgHtml));
    a.appendChild(art);

    const t = document.createElement('span');
    t.className = 'emission__title';
    t.textContent = item.title;
    a.appendChild(t);

    const f = document.createElement('span');
    f.className = 'emission__family';
    f.textContent = item.family;
    a.appendChild(f);

    const e = document.createElement('span');
    e.className = 'emission__edition';
    e.textContent = item.edition;
    a.appendChild(e);

    return a;
  }

  function renderPosterCard(item) {
    const a = document.createElement('a');
    a.className = 'poster-card';
    a.href = item.href || '#';

    const art = document.createElement('div');
    art.className = 'poster-card__art';
    const svgHtml = ART_TEMPLATES[item.art] ? ART_TEMPLATES[item.art]() : '';
    if (svgHtml) art.appendChild(svgFrom(svgHtml));
    a.appendChild(art);

    const body = document.createElement('div');
    body.className = 'poster-card__body';

    const title = document.createElement('h3');
    title.className = 'poster-card__title';
    title.textContent = item.title;
    body.appendChild(title);

    const release = document.createElement('p');
    release.className = 'poster-card__release';
    release.textContent = item.release + ' · ' + item.family;
    body.appendChild(release);

    const specs = document.createElement('div');
    specs.className = 'poster-card__specs';
    [['Format', item.format], ['Print', item.print], ['Edition', 'N° ' + item.edition], ['Price', item.price]].forEach(([k, v]) => {
      const row = document.createElement('div');
      const lbl = document.createElement('span');
      lbl.className = 'label';
      lbl.textContent = k;
      const val = document.createElement('span');
      val.className = 'val';
      val.textContent = v;
      row.appendChild(lbl);
      row.appendChild(val);
      specs.appendChild(row);
    });
    body.appendChild(specs);

    a.appendChild(body);
    return a;
  }

  function renderCollectionCard(item) {
    const a = document.createElement('a');
    a.className = 'collection-card';
    a.href = item.href || '#';

    const art = document.createElement('div');
    art.className = 'collection-card__art';
    const svgHtml = ART_TEMPLATES[item.art] ? ART_TEMPLATES[item.art]() : '';
    if (svgHtml) art.appendChild(svgFrom(svgHtml));
    a.appendChild(art);

    const body = document.createElement('div');
    body.className = 'collection-card__body';

    const title = document.createElement('h3');
    title.className = 'collection-card__title';
    title.textContent = item.title;
    body.appendChild(title);

    const tag = document.createElement('p');
    tag.className = 'collection-card__tagline';
    tag.textContent = item.tagline;
    body.appendChild(tag);

    const foot = document.createElement('p');
    foot.className = 'collection-card__foot';
    foot.textContent = item.family + ' · ' + item.count + ' items';
    body.appendChild(foot);

    a.appendChild(body);
    return a;
  }

  function renderDispatch(item, lang) {
    const a = document.createElement('a');
    a.className = 'dispatch';
    a.href = item.href || '#';

    const cover = document.createElement('div');
    cover.className = 'dispatch__cover';
    const svgHtml = ART_TEMPLATES[item.art] ? ART_TEMPLATES[item.art]() : '';
    if (svgHtml) cover.appendChild(svgFrom(svgHtml));
    a.appendChild(cover);

    const body = document.createElement('div');
    body.className = 'dispatch__body';

    const eyebrow = document.createElement('p');
    eyebrow.className = 'dispatch__eyebrow';
    const eyebrowNum = document.createElement('span');
    eyebrowNum.className = 'num';
    eyebrowNum.textContent = item.number;
    eyebrow.append('Dispatch ', eyebrowNum, ` · ${item.published} · ${item.duration}`);
    body.appendChild(eyebrow);

    const title = document.createElement('h2');
    title.className = 'dispatch__title';
    title.dataset.en = item.title_en;
    title.dataset.es = item.title_es;
    title.textContent = lang === 'es' ? item.title_es : item.title_en;
    body.appendChild(title);

    const summary = document.createElement('p');
    summary.className = 'dispatch__summary';
    summary.dataset.en = item.summary_en;
    summary.dataset.es = item.summary_es;
    summary.textContent = lang === 'es' ? item.summary_es : item.summary_en;
    body.appendChild(summary);

    const sig = document.createElement('div');
    sig.className = 'dispatch__signature';
    const signer = document.createElement('span');
    signer.className = 'dispatch__signer';
    signer.textContent = '— SORA · Founder & Signal Keeper';
    sig.appendChild(signer);
    const listen = document.createElement('span');
    listen.className = 'dispatch__listen';
    const listenSvg = svgFrom('<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" xmlns="http://www.w3.org/2000/svg"><path d="M4 3.5v9l8-4.5z"/></svg>');
    listen.appendChild(listenSvg);
    listen.append(' Listen');
    sig.appendChild(listen);
    body.appendChild(sig);

    a.appendChild(body);
    return a;
  }

  function formatTitle(s) {
    if (!s) return s;
    return s.replace(/'/g, "'");
  }

  async function loadCatalog() {
    let data;
    try {
      const res = await fetch(CATALOG_URL);
      if (!res.ok) throw new Error('catalog fetch failed: ' + res.status);
      data = await res.json();
    } catch (e) {
      console.error('[catalog] load failed', e);
      return;
    }

    // rows
    document.querySelectorAll('[data-row]').forEach((host) => {
      const rowName = host.dataset.row;
      const items = (data.rows && data.rows[rowName]) || [];
      const showEdition = host.dataset.showEdition === 'true';
      const frag = document.createDocumentFragment();
      items.forEach((item) => frag.appendChild(renderRowCard(item, { showEdition })));
      host.appendChild(frag);
    });

    // full catalog grid (explore page)
    document.querySelectorAll('[data-list="all-releases"]').forEach((host) => {
      const all = [];
      ['sora','tonswap','solswap','memes'].forEach(rn => {
        ((data.rows && data.rows[rn]) || []).forEach(it => all.push({ ...it, _row: rn }));
      });
      const frag = document.createDocumentFragment();
      all.forEach((item) => frag.appendChild(renderRowCard(item, { showEdition: !!item.edition })));
      host.appendChild(frag);
      host.dispatchEvent(new CustomEvent('catalog:all-rendered', { detail: { items: all } }));
    });

    // queue
    document.querySelectorAll('[data-list="queue"]').forEach((host) => {
      const items = data.queue || [];
      const frag = document.createDocumentFragment();
      items.forEach((item, i) => frag.appendChild(renderQueueCard(item, i)));
      // viewall anchor preserved if already present (rendered before by host)
      host.appendChild(frag);
      host.dispatchEvent(new CustomEvent('catalog:queue-rendered', { detail: { items } }));
    });

    // emissions
    document.querySelectorAll('[data-list="emissions"]').forEach((host) => {
      const items = data.emissions || [];
      const frag = document.createDocumentFragment();
      items.forEach((item) => frag.appendChild(renderEmission(item)));
      host.appendChild(frag);
    });

    // posters
    document.querySelectorAll('[data-list="posters"]').forEach((host) => {
      const items = data.posters || [];
      const frag = document.createDocumentFragment();
      items.forEach((item) => frag.appendChild(renderPosterCard(item)));
      host.appendChild(frag);
    });

    // collections
    document.querySelectorAll('[data-list="collections"]').forEach((host) => {
      const items = data.collections || [];
      const frag = document.createDocumentFragment();
      items.forEach((item) => frag.appendChild(renderCollectionCard(item)));
      host.appendChild(frag);
    });

    // journal dispatches
    document.querySelectorAll('[data-list="journal"]').forEach((host) => {
      const items = data.journal || [];
      const lang = document.documentElement.lang === 'es' ? 'es' : 'en';
      const frag = document.createDocumentFragment();
      items.forEach((item) => frag.appendChild(renderDispatch(item, lang)));
      host.appendChild(frag);
      host.dispatchEvent(new CustomEvent('catalog:journal-rendered', { detail: { items } }));
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadCatalog);
  } else {
    loadCatalog();
  }
})();
