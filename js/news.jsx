/* global React, useT, useLang, NeonVisualizer, PageHeader */
// Sora News — podcast section. Self-contained audio player with the same
// neon visualizer as Studio, but the audio graph and state live here (no
// shared provider). News content is foreground listening — when the user
// leaves the section the audio stops (unmount cleanup). Language follows
// the global i18n: 'es' → ES track, anything else → EN.
const { useState, useEffect, useRef, useMemo } = React;

const NEWS_FALLBACK = [
  {
    slug: 'episodio-2',
    published_at: '2026-05-23T18:00:00Z',
    title_es: 'El tweetstorm de Makoto Takemiya',
    title_en: "Makoto Takemiya's tweetstorm",
    summary_es: 'Análisis de los 4 artículos publicados por Makoto Takemiya en la semana del 10 de mayo de 2026.',
    summary_en: 'Breakdown of the 4 articles Makoto Takemiya posted during the week of May 10, 2026.',
    cover_path: '/news/media/episodio-2/cover.jpeg',
    audio_path_es: '/news/media/episodio-2/es.mp3',
    audio_path_en: '/news/media/episodio-2/en.mp3',
    video_path_es: '/news/media/episodio-2/es.mp4',
    video_path_en: '/news/media/episodio-2/en.mp4',
    duration_s: 376,
  },
  {
    slug: 'episodio-1',
    published_at: '2026-05-23T12:00:00Z',
    title_es: 'Por qué SORA Nexus existe',
    title_en: 'Why SORA Nexus exists',
    summary_es: 'Marco filosófico de Fisher 1939, Yamaguchi y la respuesta de SORA al sistema monetario actual.',
    summary_en: "The philosophical framework from Fisher 1939, Yamaguchi, and SORA's answer to the current monetary system.",
    cover_path: '/news/media/episodio-1/cover.jpg',
    audio_path_es: '/news/media/episodio-1/es.mp3',
    audio_path_en: '/news/media/episodio-1/en.mp3',
    video_path_es: '/news/media/episodio-1/es.mp4',
    video_path_en: '/news/media/episodio-1/en.mp4',
    duration_s: 295,
  },
];

// Static cache buster — appended to media URLs so CloudFlare treats them as
// new resources, avoiding stale 404s cached before the files were deployed.
// Bump the suffix when redeploying media to force a fresh fetch.
const MEDIA_VER = 'v1';
function bust(url) { return url ? url + (url.includes('?') ? '&' : '?') + 'v=' + MEDIA_VER : url; }

function fmtMMSS(s) {
  s = Math.max(0, Math.floor(Number.isFinite(s) ? s : 0));
  return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
}

function fmtDate(iso, lang) {
  try {
    const d = new Date(iso);
    return d.toLocaleDateString(lang === 'es' ? 'es-ES' : 'en-US', {
      year: 'numeric', month: 'short', day: 'numeric',
    });
  } catch { return iso || ''; }
}

function NewsSection() {
  const { lang: uiLang, t } = useLang();

  const [episodes, setEpisodes] = useState(NEWS_FALLBACK);
  const [idx, setIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [volume, setVolume] = useState(0.85);
  const [freq, setFreq] = useState(null);

  const audioRef = useRef(null);
  const ctxRef = useRef(null);
  const analyserRef = useRef(null);
  const sourceRef = useRef(null);
  const gainRef = useRef(null);
  const rafRef = useRef(0);

  // Load real episodes from the API, fall back to the inline list if it 404s
  // (e.g. local preview without backend).
  useEffect(() => {
    let cancelled = false;
    fetch('/news/episodes').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled) return;
      if (j && Array.isArray(j.episodes) && j.episodes.length) {
        setEpisodes(j.episodes);
      }
    }).catch(() => {});
    return () => { cancelled = true; };
  }, []);

  const ep = episodes[idx] || episodes[0];
  const audioLang = uiLang === 'es' ? 'es' : 'en';
  const audioSrc = ep ? bust(audioLang === 'es' ? ep.audio_path_es : ep.audio_path_en) : null;
  const title = ep ? (uiLang === 'es' ? ep.title_es : ep.title_en) : '';
  const summary = ep ? (uiLang === 'es' ? ep.summary_es : ep.summary_en) : '';

  // Load track when episode or language changes. Pause if switching.
  useEffect(() => {
    const el = audioRef.current;
    if (!el || !audioSrc) return;
    el.src = audioSrc;
    el.load();
    setElapsed(0);
  }, [audioSrc]);

  // Build audio graph lazily on first play (Web Audio needs a user gesture).
  const ensureGraph = () => {
    const el = audioRef.current;
    if (!el || sourceRef.current) {
      ctxRef.current?.resume();
      return;
    }
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      const ctx = new Ctx();
      const src = ctx.createMediaElementSource(el);
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 256;
      analyser.smoothingTimeConstant = 0.82;
      const gain = ctx.createGain();
      gain.gain.value = volume;
      src.connect(analyser);
      analyser.connect(gain);
      gain.connect(ctx.destination);
      ctxRef.current = ctx;
      analyserRef.current = analyser;
      sourceRef.current = src;
      gainRef.current = gain;
    } catch {}
  };

  // Play / pause.
  useEffect(() => {
    const el = audioRef.current;
    if (!el) return;
    if (playing) {
      ensureGraph();
      el.play().catch(() => setPlaying(false));
    } else {
      el.pause();
    }
  }, [playing, audioSrc]);

  // Volume — same pre-graph / post-graph dance as Studio.
  useEffect(() => {
    const el = audioRef.current;
    if (el) el.volume = volume;
    if (gainRef.current) gainRef.current.gain.value = volume;
  }, [volume]);

  // Timebase + auto-pause at end (no auto-advance; user picks the next ep).
  useEffect(() => {
    const el = audioRef.current;
    if (!el) return;
    const onTime = () => setElapsed(el.currentTime || 0);
    const onEnd = () => setPlaying(false);
    el.addEventListener('timeupdate', onTime);
    el.addEventListener('ended', onEnd);
    return () => {
      el.removeEventListener('timeupdate', onTime);
      el.removeEventListener('ended', onEnd);
    };
  }, []);

  // Drive frequency data while playing.
  useEffect(() => {
    if (!playing) { cancelAnimationFrame(rafRef.current); return; }
    const tick = () => {
      const a = analyserRef.current;
      if (a) {
        const arr = new Uint8Array(a.frequencyBinCount);
        a.getByteFrequencyData(arr);
        setFreq(arr);
      }
      rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
  }, [playing]);

  // Stop and tear down the audio context when leaving the section.
  useEffect(() => () => {
    try { audioRef.current?.pause(); } catch {}
    try { ctxRef.current?.close(); } catch {}
  }, []);

  const dur = audioRef.current?.duration || ep?.duration_s || 0;
  const progress = dur > 0 ? elapsed / dur : 0;
  const onSeek = (e) => {
    const el = audioRef.current;
    if (!el || !dur) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const p = (e.clientX - rect.left) / rect.width;
    el.currentTime = p * dur;
    setElapsed(p * dur);
  };

  const energy = useMemo(() => {
    if (!freq || freq.length === 0) return 0;
    let sum = 0;
    const lim = Math.min(32, freq.length);
    for (let i = 0; i < lim; i++) sum += freq[i];
    return sum / (lim * 255);
  }, [freq]);

  const pickEpisode = (i) => { setIdx(i); setPlaying(true); };

  // Slug → episode number for the "EPISODIO N" tag. Falls back to the
  // reverse index so newer schemes still produce a label.
  const episodeNumber = (slug, fallbackIndex) => {
    const m = (slug || '').match(/(\d+)/);
    return m ? Number(m[1]) : (episodes.length - fallbackIndex);
  };

  // Countdown overlay during the first 30s of playback (audio is muted during
  // those seconds in the production mix — voice enters at t=25, dominates by
  // t=30, so the overlay disappears as the voice takes over).
  const COUNTDOWN_LEN = 30;
  const showCountdown = playing && elapsed < COUNTDOWN_LEN;
  const countdownValue = Math.max(1, COUNTDOWN_LEN - Math.floor(elapsed));

  return (
    <div>
      <PageHeader title={t('nav.news')} sub={t('news.subtitle')}>
        <span className="tag" style={{display:'flex', alignItems:'center', gap:6}}>
          <span className="live-dot" style={{width:6, height:6, background: playing ? '#10B981' : '#6B7280'}}/>
          {playing ? t('news.nowPlaying') : '—'} · {episodes.length} ep
        </span>
      </PageHeader>

      <audio ref={audioRef} preload="metadata" crossOrigin="anonymous" style={{display:'none'}}/>

      <div style={{
        position:'relative', borderRadius: 18, overflow:'hidden',
        border: '1px solid rgba(236,72,153,0.25)',
        boxShadow: `0 0 ${40 + energy * 80}px rgba(236,72,153,${0.18 + energy * 0.4}), 0 0 ${20 + energy * 40}px rgba(96,165,250,${0.12 + energy * 0.25}) inset`,
        transition: 'box-shadow 80ms linear',
        background: '#0a0a14',
        maxWidth: 960, width: '100%', margin: '0 auto',
      }}>
        <div style={{position:'relative', width:'100%', aspectRatio:'3/2', background: '#000'}}>
          {ep?.cover_path && (
            <img src={bust(ep.cover_path)} alt={title}
                 style={{position:'absolute', inset:0, width:'100%', height:'100%', objectFit:'cover', filter: playing ? 'brightness(0.78)' : 'brightness(0.92)', transition:'filter 200ms ease'}}/>
          )}
          {/* Visualizer overlay: bottom strip, only visible while audio is alive */}
          <div style={{position:'absolute', left:0, right:0, bottom:0, height:'38%', pointerEvents:'none', opacity: playing || elapsed > 0 ? 1 : 0.35, transition:'opacity 240ms ease'}}>
            <NeonVisualizer freq={freq} playing={playing} accent="#EC4899" accent2="#60A5FA"/>
          </div>
          {/* Countdown overlay during the first 30s of playback — disappears as
              the voice takes over from the music intro at t=30s */}
          {showCountdown && (
            <div style={{position:'absolute', inset:0, display:'flex', alignItems:'center', justifyContent:'center', pointerEvents:'none'}}>
              <div style={{
                fontSize:'clamp(120px, 22vw, 320px)', fontWeight:900, color:'rgba(255,255,255,0.92)',
                textShadow:'0 0 30px rgba(0,0,0,0.85), 6px 6px 0 rgba(0,0,0,0.5)',
                fontFamily:'"Arial Black", Impact, Helvetica, sans-serif', lineHeight:1,
                letterSpacing:'-0.04em',
              }}>{countdownValue}</div>
            </div>
          )}
          {/* Title overlay */}
          <div style={{position:'absolute', top:18, left:22, right:22, display:'flex', alignItems:'flex-start', justifyContent:'space-between', gap:16, pointerEvents:'none'}}>
            <div>
              <div style={{fontSize:11, letterSpacing:'0.3em', color:'#EC4899', textTransform:'uppercase'}}>{t('news.nowPlaying')}</div>
              <div style={{fontSize:26, fontWeight:800, color:'#fff', marginTop:2, textShadow:'0 0 18px rgba(0,0,0,0.7)', maxWidth: '70%'}}>{title || '—'}</div>
              <div style={{fontSize:13, color:'rgba(255,255,255,0.78)', marginTop:4, textShadow:'0 0 12px rgba(0,0,0,0.6)'}}>{fmtDate(ep?.published_at, uiLang)} · {audioLang.toUpperCase()}</div>
            </div>
          </div>
          {/* Center play button overlay */}
          <button
            onClick={() => { ensureGraph(); setPlaying(p => !p); }}
            title={playing ? 'Pause' : 'Play'}
            style={{
              position:'absolute', top:'50%', left:'50%', transform:'translate(-50%, -50%)',
              width: 78, height: 78, borderRadius: '50%', border: '2px solid rgba(255,255,255,0.85)',
              background: playing ? 'rgba(236,72,153,0.85)' : 'rgba(0,0,0,0.45)',
              color: '#fff', fontSize: 30, cursor: 'pointer',
              backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)',
              boxShadow: '0 0 30px rgba(236,72,153,0.5)',
              display:'flex', alignItems:'center', justifyContent:'center',
            }}>
            {playing ? '⏸' : '▶'}
          </button>
        </div>

        <div style={{padding:'16px 24px 20px', background:'rgba(0,0,0,0.5)', borderTop:'1px solid rgba(255,255,255,0.06)'}}>
          <div onClick={onSeek} style={{height:6, borderRadius:3, background:'rgba(255,255,255,0.08)', cursor:'pointer', overflow:'hidden', marginBottom:12}}>
            <div style={{height:'100%', width:(progress * 100) + '%', background:'linear-gradient(90deg, #EC4899, #60A5FA)', boxShadow:'0 0 12px rgba(236,72,153,0.6)', transition:'width 120ms linear'}}/>
          </div>
          <div style={{display:'flex', alignItems:'center', gap:14, flexWrap:'wrap'}}>
            <div className="num tiny" style={{color:'rgba(255,255,255,0.7)', minWidth:100, fontFamily:'JetBrains Mono'}}>
              {fmtMMSS(elapsed)} <span style={{opacity:0.5}}>/ {fmtMMSS(dur)}</span>
            </div>
            <div style={{flex:1, color:'rgba(255,255,255,0.7)', fontSize:13}}>{summary}</div>
            <div style={{display:'flex', alignItems:'center', gap:8}}>
              <span className="muted tiny">VOL</span>
              <input type="range" min="0" max="1" step="0.01" value={volume} onChange={e => setVolume(Number(e.target.value))} style={{width:110, accentColor:'#EC4899'}}/>
            </div>
          </div>
        </div>
      </div>

      <div className="card" style={{marginTop: 20, maxWidth: 960, marginLeft:'auto', marginRight:'auto'}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> {t('news.episodes')}</div>
        </div>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft:20, width:44}}>#</th>
                <th style={{width:56}}></th>
                <th>{t('news.episodes')}</th>
                <th style={{width:140}}>{uiLang === 'es' ? 'Fecha' : 'Date'}</th>
                <th style={{textAlign:'right', paddingRight:20, width:80}}>Dur</th>
              </tr>
            </thead>
            <tbody>
              {episodes.map((e, i) => {
                const isActive = i === idx;
                const epTitle = uiLang === 'es' ? e.title_es : e.title_en;
                const epNum = episodeNumber(e.slug, i);
                const epLabel = (uiLang === 'es' ? 'Episodio ' : 'Episode ') + epNum;
                return (
                  <tr key={e.slug} className="swap-row clickable" onClick={() => pickEpisode(i)}
                      style={{background: isActive ? 'rgba(236,72,153,0.08)' : undefined}}>
                    <td style={{paddingLeft:20}}>
                      {isActive && playing
                        ? <span style={{color:'#EC4899', fontSize:14}}>♪</span>
                        : <span className="muted tiny">{i + 1}</span>}
                    </td>
                    <td>
                      {e.cover_path && (
                        <img src={bust(e.cover_path)} alt=""
                             style={{width:40, height:40, objectFit:'cover', borderRadius:4, display:'block'}}/>
                      )}
                    </td>
                    <td>
                      <div style={{fontSize:9, letterSpacing:'0.22em', color:'#EC4899', textTransform:'uppercase', fontWeight:700, marginBottom:2}}>{epLabel}</div>
                      <div style={{fontWeight: isActive ? 700 : 500, color: isActive ? '#fff' : 'var(--fg-1)'}}>{epTitle}</div>
                    </td>
                    <td className="muted tiny">{fmtDate(e.published_at, uiLang)}</td>
                    <td style={{textAlign:'right', paddingRight:20}} className="num tiny muted">
                      {e.duration_s ? fmtMMSS(e.duration_s) : '—'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { NewsSection });
