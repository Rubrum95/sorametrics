/* global React, useT, fmt, PageHeader */
// Music studio — a full-page audio player with a neon-reactive visualizer,
// plus a persistent <audio> element hoisted into a top-level <StudioProvider/>
// so playback survives section changes. When the user is NOT on the Studio
// page but audio is playing, we render a compact floating mini-player
// (dismissable; closing it stops the music).
const { useState, useEffect, useRef, useMemo, useContext, createContext } = React;

/* ==========================================================================
   Context + audio graph
   ========================================================================== */
const StudioContext = createContext(null);
function useStudio() { return useContext(StudioContext); }

const STUDIO_FALLBACK = [
  { title: 'Sakura no Yume',     artist: 'Yumiko Tanaka',  dur: 214 },
  { title: 'Midnight Tokyo',     artist: 'Kanade',         dur: 186 },
  { title: 'Lo-fi XOR',          artist: 'Sora Collective',dur: 248 },
  { title: 'Blockchain Bloom',   artist: 'ambient.wav',    dur: 302 },
  { title: 'Validator Dreams',   artist: 'Kusari',         dur: 224 },
  { title: 'Bridge Lullaby',     artist: 'Cerberus',       dur: 278 },
];

function fmtMMSS(s) {
  s = Math.max(0, Math.floor(s));
  return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
}

function StudioProvider({ children, section, setSection }) {
  const audioRef = useRef(null);
  // Persistent state — survives route/section changes because the provider
  // wraps the whole app.
  const [tracks, setTracks] = useState(STUDIO_FALLBACK);
  const [idx, setIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [volume, setVolume] = useState(0.75);
  // Playback modes — classic player semantics:
  //   repeat: 'off' | 'all' | 'one'
  //     off → stop after the last track.
  //     all → wrap to the first track after the last.
  //     one → replay the current track forever.
  //   shuffle: boolean — both next() and auto-advance pick a random index
  //     (avoiding the current one so it doesn't get stuck).
  // Persisted to localStorage so the user's preference survives reloads.
  const [repeat, setRepeat] = useState(() => {
    try { const v = localStorage.getItem('sm.studio.repeat'); return v === 'all' || v === 'one' ? v : 'off'; }
    catch { return 'off'; }
  });
  const [shuffle, setShuffle] = useState(() => {
    try { return localStorage.getItem('sm.studio.shuffle') === '1'; }
    catch { return false; }
  });
  useEffect(() => { try { localStorage.setItem('sm.studio.repeat', repeat); } catch {} }, [repeat]);
  useEffect(() => { try { localStorage.setItem('sm.studio.shuffle', shuffle ? '1' : '0'); } catch {} }, [shuffle]);
  // Analyser plumbing — built the first time the user hits play (Web Audio
  // requires a user gesture to create/resume the context).
  const ctxRef = useRef(null);
  const analyserRef = useRef(null);
  const sourceRef = useRef(null);
  const [freq, setFreq] = useState(null);
  const rafRef = useRef(0);

  // Fetch real playlist.
  useEffect(() => {
    let cancelled = false;
    fetch('/music/list').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled || !Array.isArray(j) || j.length === 0) return;
      setTracks(j.map(x => ({ title: x.title, artist: x.artist, src: x.src, dur: 180 })));
    }).catch(() => {});
    return () => { cancelled = true; };
  }, []);

  const track = tracks[idx] || tracks[0];

  // Wire src + play state.
  useEffect(() => {
    const el = audioRef.current;
    if (!el) return;
    if (track?.src && el.src !== track.src) {
      el.src = track.src;
      el.load();
    }
    el.volume = volume;
    if (playing) {
      ensureAudioGraph();
      el.play().catch(() => setPlaying(false));
    } else {
      el.pause();
    }
  }, [track?.src, playing, volume]);

  // Timebase + auto-advance. Repeat/shuffle change the 'ended' behaviour,
  // so they're in the dep list to re-register the handler with fresh values.
  useEffect(() => {
    const el = audioRef.current;
    if (!el) return;
    const onTime = () => setElapsed(el.currentTime || 0);
    const onEnd = () => {
      const n = tracks.length;
      if (n <= 0) return;
      if (repeat === 'one') {
        // Replay the same track from 0. No setIdx — keep the same index so
        // the [track?.src] effect doesn't reload the file unnecessarily.
        el.currentTime = 0;
        el.play().catch(() => setPlaying(false));
        return;
      }
      if (shuffle && n > 1) {
        setIdx(curr => {
          let r; do { r = Math.floor(Math.random() * n); } while (r === curr);
          return r;
        });
        return;
      }
      setIdx(curr => {
        const ni = curr + 1;
        if (ni >= n) {
          if (repeat === 'all') return 0;
          // repeat 'off' and we just finished the last track → stop playback.
          setPlaying(false);
          return curr;
        }
        return ni;
      });
    };
    el.addEventListener('timeupdate', onTime);
    el.addEventListener('ended', onEnd);
    return () => {
      el.removeEventListener('timeupdate', onTime);
      el.removeEventListener('ended', onEnd);
    };
  }, [tracks.length, repeat, shuffle]);

  useEffect(() => { setElapsed(0); }, [idx]);

  // Build the analyser graph lazily — calling createMediaElementSource twice
  // on the same element throws, hence the ref guard.
  const ensureAudioGraph = () => {
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
      src.connect(analyser);
      analyser.connect(ctx.destination);
      ctxRef.current = ctx;
      analyserRef.current = analyser;
      sourceRef.current = src;
    } catch (err) {
      // Silently fall back — the page still works, just without the visualizer.
    }
  };

  // Drive the frequency array on a RAF loop while playing; sleep while paused.
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

  const dur = audioRef.current?.duration || track?.dur || 180;
  const seek = (p) => {
    const el = audioRef.current;
    if (!el) return;
    el.currentTime = p * dur;
    setElapsed(p * dur);
  };
  // next()/prev() respect shuffle: a random index different from the current
  // one. Without shuffle, they wrap around the tracklist as before — the
  // 'repeat off' semantic only applies to auto-advance (onEnd), not to
  // user-driven Next/Prev (matches Spotify/Apple Music behaviour).
  const pickRandomIdx = (curr) => {
    const n = tracks.length;
    if (n <= 1) return 0;
    let r; do { r = Math.floor(Math.random() * n); } while (r === curr);
    return r;
  };
  const next = () => setIdx(i => shuffle ? pickRandomIdx(i) : (i + 1) % Math.max(1, tracks.length));
  const prev = () => setIdx(i => shuffle ? pickRandomIdx(i) : (i - 1 + tracks.length) % Math.max(1, tracks.length));
  // Cycles through off → all → one → off.
  const cycleRepeat = () => setRepeat(r => r === 'off' ? 'all' : r === 'all' ? 'one' : 'off');
  const toggleShuffle = () => setShuffle(s => !s);
  const play = () => { ensureAudioGraph(); setPlaying(true); };
  const pause = () => setPlaying(false);
  const toggle = () => (playing ? pause() : play());
  const selectIdx = (i) => { setIdx(i); play(); };
  const stop = () => {
    setPlaying(false);
    const el = audioRef.current;
    if (el) { el.pause(); el.currentTime = 0; }
    setElapsed(0);
  };

  const api = {
    tracks, idx, track, playing, elapsed, volume, dur, freq,
    setVolume, play, pause, toggle, prev, next, selectIdx, seek, stop,
    repeat, shuffle, cycleRepeat, toggleShuffle,
    section, setSection,
  };

  return (
    <StudioContext.Provider value={api}>
      {/* Audio element lives at the root so it keeps playing across section
          changes. crossOrigin needed for the AnalyserNode to read samples. */}
      <audio ref={audioRef} preload="metadata" crossOrigin="anonymous" style={{display:'none'}}/>
      {children}
      {/* Mini-player: only visible off the Studio page AND when something is
          loaded (playing or paused mid-track). */}
      {section !== 'studio' && (playing || elapsed > 0) && <StudioMiniPlayer/>}
    </StudioContext.Provider>
  );
}

/* ==========================================================================
   Visualizer canvas
   ========================================================================== */
function NeonVisualizer({ freq, playing, accent = '#EC4899', accent2 = '#60A5FA' }) {
  const canvasRef = useRef(null);
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const resize = () => {
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.floor(rect.width * dpr);
      canvas.height = Math.floor(rect.height * dpr);
    };
    resize();
    window.addEventListener('resize', resize);
    return () => window.removeEventListener('resize', resize);
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    // Synthwave floor grid.
    ctx.save();
    ctx.globalAlpha = 0.25;
    const horizon = H * 0.72;
    ctx.strokeStyle = 'rgba(155,27,48,0.35)';
    ctx.lineWidth = 1;
    for (let i = 0; i < 12; i++) {
      const y = horizon + (H - horizon) * (i / 12);
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
    }
    for (let i = -8; i <= 8; i++) {
      const x = W / 2 + (i / 8) * W * 0.9;
      ctx.beginPath(); ctx.moveTo(W / 2, horizon); ctx.lineTo(x, H); ctx.stroke();
    }
    ctx.restore();

    if (!freq) {
      ctx.save();
      ctx.globalAlpha = 0.3;
      ctx.strokeStyle = accent2;
      ctx.lineWidth = 2 * (window.devicePixelRatio || 1);
      ctx.beginPath();
      for (let x = 0; x < W; x++) {
        const y = horizon - Math.sin(x * 0.02 + Date.now() * 0.002) * 8;
        if (x === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }
      ctx.stroke();
      ctx.restore();
      return;
    }

    const bins = freq.length;
    const usable = Math.floor(bins * 0.7);
    const halfBars = usable;
    const totalBars = halfBars * 2 + 1;
    const barW = W / totalBars;
    for (let i = 0; i < halfBars; i++) {
      const v = freq[i] / 255;
      const barH = Math.pow(v, 1.25) * H * 0.62;
      const positions = [
        Math.floor((halfBars - i) * barW),
        Math.floor((halfBars + i) * barW),
      ];
      const grad = ctx.createLinearGradient(0, horizon, 0, horizon - barH);
      grad.addColorStop(0, accent);
      grad.addColorStop(1, accent2);
      ctx.fillStyle = grad;
      ctx.shadowBlur = 18 * (playing ? 1 : 0.3);
      ctx.shadowColor = accent;
      for (const x of positions) ctx.fillRect(x + 1, horizon - barH, barW - 2, barH);
      ctx.shadowBlur = 0;
      ctx.globalAlpha = 0.25;
      for (const x of positions) ctx.fillRect(x + 1, horizon, barW - 2, barH * 0.35);
      ctx.globalAlpha = 1;
    }
  }, [freq, playing, accent, accent2]);

  return <canvas ref={canvasRef} style={{width:'100%', height:'100%', display:'block'}}/>;
}

/* ==========================================================================
   Full-page section
   ========================================================================== */
function MusicStudioSection() {
  const t = useT();
  const s = useStudio();
  const [search, setSearch] = useState('');
  if (!s) return <div>Studio provider missing.</div>;

  const { tracks, idx, track, playing, elapsed, volume, dur, freq,
          setVolume, toggle, prev, next, seek, selectIdx,
          repeat, shuffle, cycleRepeat, toggleShuffle } = s;

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    const mapped = tracks.map((tr, i) => ({ ...tr, originalIdx: i }));
    return q ? mapped.filter(tr => tr.title.toLowerCase().includes(q) || (tr.artist || '').toLowerCase().includes(q)) : mapped;
  }, [tracks, search]);

  const progress = dur > 0 ? elapsed / dur : 0;
  const onSeekTrack = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    seek((e.clientX - rect.left) / rect.width);
  };

  const energy = useMemo(() => {
    if (!freq || freq.length === 0) return 0;
    let sum = 0;
    const lim = Math.min(32, freq.length);
    for (let i = 0; i < lim; i++) sum += freq[i];
    return sum / (lim * 255);
  }, [freq]);

  return (
    <div>
      <PageHeader title={t('nav.studio')} sub="Listen on-chain. Visualize off-chain.">
        <span className="tag" style={{display:'flex', alignItems:'center', gap:6}}>
          <span className="live-dot" style={{width:6, height:6, background: playing ? '#10B981' : '#6B7280'}}/>
          {playing ? 'Playing' : 'Paused'} · {tracks.length} tracks
        </span>
      </PageHeader>

      <div style={{
        position:'relative', borderRadius: 18, overflow:'hidden',
        border: '1px solid rgba(236,72,153,0.25)',
        boxShadow: `0 0 ${40 + energy * 80}px rgba(236,72,153,${0.18 + energy * 0.4}), 0 0 ${20 + energy * 40}px rgba(96,165,250,${0.12 + energy * 0.25}) inset`,
        transition: 'box-shadow 80ms linear',
        background: 'radial-gradient(120% 80% at 50% 100%, rgba(236,72,153,0.18), transparent 60%), linear-gradient(180deg, #0a0a14, #0e0816 40%, #06030c)',
      }}>
        <div style={{height: 340, position:'relative'}}>
          <NeonVisualizer freq={freq} playing={playing} accent="#EC4899" accent2="#60A5FA"/>
          <div style={{position:'absolute', top:18, left:22, right:22, display:'flex', alignItems:'flex-start', justifyContent:'space-between', gap:16, pointerEvents:'none'}}>
            <div>
              <div style={{fontSize:11, letterSpacing:'0.3em', color:'#EC4899', textTransform:'uppercase'}}>Now playing</div>
              <div style={{fontSize:26, fontWeight:800, color:'#fff', marginTop:2, textShadow:'0 0 24px rgba(236,72,153,0.6)'}}>{track?.title || '—'}</div>
              <div style={{fontSize:14, color:'rgba(255,255,255,0.7)', marginTop:2}}>{track?.artist || ''}</div>
            </div>
            <div style={{fontFamily:'JetBrains Mono', fontSize:11, color:'rgba(255,255,255,0.45)', textAlign:'right', lineHeight:1.5}}>
              <div>FFT 256</div>
              <div>48 kHz · stereo</div>
              <div>{freq ? freq.length + ' bins' : 'idle'}</div>
            </div>
          </div>
        </div>

        <div style={{padding:'18px 24px 20px', background:'rgba(0,0,0,0.45)', borderTop:'1px solid rgba(255,255,255,0.06)'}}>
          <div onClick={onSeekTrack} style={{height:6, borderRadius:3, background:'rgba(255,255,255,0.08)', cursor:'pointer', overflow:'hidden', marginBottom:14}}>
            <div style={{height:'100%', width: (progress * 100) + '%', background:'linear-gradient(90deg, #EC4899, #60A5FA)', boxShadow:'0 0 12px rgba(236,72,153,0.6)', transition:'width 120ms linear'}}/>
          </div>
          <div style={{display:'flex', alignItems:'center', gap:16, flexWrap:'wrap'}}>
            <div style={{display:'flex', alignItems:'center', gap:10}}>
              {/* Shuffle — highlights pink when on. */}
              <button
                onClick={toggleShuffle}
                title={shuffle ? 'Shuffle: on' : 'Shuffle: off'}
                aria-pressed={shuffle}
                style={{...btnStyle,
                  color: shuffle ? '#EC4899' : 'rgba(255,255,255,0.55)',
                  borderColor: shuffle ? 'rgba(236,72,153,0.5)' : 'rgba(255,255,255,0.12)',
                  boxShadow: shuffle ? '0 0 12px rgba(236,72,153,0.4)' : 'none',
                }}>🔀</button>
              <button onClick={prev} title="Previous" style={btnStyle}>⏮</button>
              <button onClick={toggle} title={playing ? 'Pause' : 'Play'} style={{...btnStyle, width:52, height:52, fontSize:22, background:'linear-gradient(135deg, #EC4899, #9B1B30)', color:'#fff', boxShadow:'0 0 24px rgba(236,72,153,0.5)'}}>
                {playing ? '⏸' : '▶'}
              </button>
              <button onClick={next} title="Next" style={btnStyle}>⏭</button>
              {/* Repeat — cycles off → all → one. Emoji swaps for 'one'. */}
              <button
                onClick={cycleRepeat}
                title={'Repeat: ' + repeat}
                aria-pressed={repeat !== 'off'}
                style={{...btnStyle,
                  color: repeat === 'off' ? 'rgba(255,255,255,0.55)' : '#EC4899',
                  borderColor: repeat === 'off' ? 'rgba(255,255,255,0.12)' : 'rgba(236,72,153,0.5)',
                  boxShadow: repeat === 'off' ? 'none' : '0 0 12px rgba(236,72,153,0.4)',
                }}>{repeat === 'one' ? '🔂' : '🔁'}</button>
            </div>
            <div className="num tiny" style={{color:'rgba(255,255,255,0.65)', minWidth:100}}>
              {fmtMMSS(elapsed)} <span style={{opacity:0.5}}>/ {fmtMMSS(dur)}</span>
            </div>
            <div style={{flex:1}}/>
            <div style={{display:'flex', alignItems:'center', gap:8}}>
              <span className="muted tiny">VOL</span>
              <input type="range" min="0" max="1" step="0.01" value={volume} onChange={e => setVolume(Number(e.target.value))} style={{width:120, accentColor:'#EC4899'}}/>
              <span className="num tiny" style={{width:32, textAlign:'right', color:'rgba(255,255,255,0.65)'}}>{Math.round(volume * 100)}</span>
            </div>
          </div>
        </div>
      </div>

      <div className="card" style={{marginTop: 20}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> Tracklist</div>
          <input type="text" placeholder="Search title or artist…" value={search} onChange={e => setSearch(e.target.value)}
                 style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, width:240, outline:'none'}}/>
        </div>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft:20, width:44}}>#</th>
                <th>Title</th>
                <th>Artist</th>
                <th style={{textAlign:'right', paddingRight:20, width:80}}>Dur</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 && (
                <tr><td colSpan={4} style={{padding:24, textAlign:'center', color:'var(--fg-2)'}}>No matches.</td></tr>
              )}
              {filtered.map(tr => {
                const isActive = tr.originalIdx === idx;
                return (
                  <tr key={tr.originalIdx} className="swap-row clickable" onClick={() => selectIdx(tr.originalIdx)} style={{background: isActive ? 'rgba(236,72,153,0.08)' : undefined}}>
                    <td style={{paddingLeft:20}}>
                      {isActive && playing
                        ? <span style={{color:'#EC4899', fontSize:14}}>♪</span>
                        : <span className="muted tiny">{tr.originalIdx + 1}</span>}
                    </td>
                    <td style={{fontWeight: isActive ? 700 : 500, color: isActive ? '#fff' : 'var(--fg-1)'}}>{tr.title}</td>
                    <td className="muted tiny">{tr.artist}</td>
                    <td style={{textAlign:'right', paddingRight:20}} className="num tiny muted">{fmtMMSS(tr.dur || 0)}</td>
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

/* ==========================================================================
   Floating mini-player (only shown when NOT on the Studio page)
   ========================================================================== */
function StudioMiniPlayer() {
  const s = useStudio();
  if (!s) return null;
  const { track, playing, elapsed, dur, toggle, next, prev, seek, stop, setSection } = s;
  const progress = dur > 0 ? elapsed / dur : 0;
  const onSeek = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    seek((e.clientX - rect.left) / rect.width);
  };

  return (
    <div style={{
      position:'fixed', top:16, right:16, zIndex:800,
      width: 320,
      background:'linear-gradient(135deg, rgba(22,8,24,0.95), rgba(12,6,20,0.95))',
      border:'1px solid rgba(236,72,153,0.35)',
      borderRadius:14,
      boxShadow:'0 20px 60px rgba(0,0,0,0.5), 0 0 40px rgba(236,72,153,0.18)',
      color:'#fff',
      backdropFilter:'blur(8px)',
      WebkitBackdropFilter:'blur(8px)',
    }}>
      <div style={{display:'flex', alignItems:'center', gap:10, padding:'10px 12px', borderBottom:'1px solid rgba(255,255,255,0.06)'}}>
        <button
          onClick={() => setSection?.('studio')}
          title="Open Studio"
          style={{flex:1, textAlign:'left', background:'transparent', border:0, color:'inherit', cursor:'pointer', padding:0, minWidth:0}}>
          <div style={{fontSize:10, letterSpacing:'0.3em', color:'#EC4899', textTransform:'uppercase'}}>Studio</div>
          <div style={{fontSize:13, fontWeight:700, whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis'}}>{track?.title || '—'}</div>
          <div style={{fontSize:11, color:'rgba(255,255,255,0.6)', whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis'}}>{track?.artist || ''}</div>
        </button>
        <button
          onClick={stop}
          title="Stop and hide"
          style={{background:'transparent', border:'1px solid rgba(255,255,255,0.15)', color:'rgba(255,255,255,0.8)', width:26, height:26, borderRadius:6, cursor:'pointer', fontSize:14, lineHeight:1}}>
          ×
        </button>
      </div>
      <div onClick={onSeek} style={{height:4, background:'rgba(255,255,255,0.08)', cursor:'pointer', overflow:'hidden'}}>
        <div style={{height:'100%', width:(progress*100)+'%', background:'linear-gradient(90deg, #EC4899, #60A5FA)', transition:'width 120ms linear'}}/>
      </div>
      <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', padding:'8px 12px', gap:8}}>
        <div style={{display:'flex', gap:6}}>
          <button onClick={prev} title="Previous" style={miniBtn}>⏮</button>
          <button onClick={toggle} title={playing ? 'Pause' : 'Play'} style={{...miniBtn, width:34, height:34, background:'linear-gradient(135deg, #EC4899, #9B1B30)', color:'#fff'}}>
            {playing ? '⏸' : '▶'}
          </button>
          <button onClick={next} title="Next" style={miniBtn}>⏭</button>
        </div>
        <div className="num tiny" style={{color:'rgba(255,255,255,0.6)', fontFamily:'JetBrains Mono'}}>
          {fmtMMSS(elapsed)} / {fmtMMSS(dur)}
        </div>
      </div>
    </div>
  );
}

const btnStyle = {
  width: 40, height: 40, borderRadius: '50%',
  border: '1px solid rgba(255,255,255,0.12)',
  background: 'rgba(255,255,255,0.04)',
  color: 'rgba(255,255,255,0.9)',
  fontSize: 16, cursor: 'pointer',
  display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
};
const miniBtn = {
  width: 28, height: 28, borderRadius: '50%',
  border: '1px solid rgba(255,255,255,0.12)',
  background: 'rgba(255,255,255,0.04)',
  color: 'rgba(255,255,255,0.85)',
  fontSize: 12, cursor: 'pointer',
  display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
};

Object.assign(window, { MusicStudioSection, StudioProvider, useStudio, StudioMiniPlayer });
