/* global React, useT, BackupRestore */
const { useState, useEffect, useRef } = React;

function TweaksPanel({ tweaks, setTweak, open, onClose }) {
  if (!open) return null;
  const t = useT();

  const opt = (group, val, label) => (
    <button
      key={val}
      className={'tweaks-opt' + (tweaks[group] === val ? ' active' : '')}
      onClick={() => setTweak(group, val)}
    >{label || val}</button>
  );

  const accents = [
    { id: 'ember', grad: 'linear-gradient(135deg, #FF4E3C, #E5243B, #B91C30)' },
    { id: 'plum',  grad: 'linear-gradient(135deg, #B862C9, #7B5B90, #4A3566)' },
    { id: 'violet',grad: 'linear-gradient(135deg, #EC4899, #8B5CF6, #4C1D95)' },
    { id: 'amber', grad: 'linear-gradient(135deg, #F59E0B, #EF4444, #7F1D1D)' },
  ];

  return (
    <div className="tweaks-panel open">
      <div className="tweaks-head">
        <h3>{t('tweaks.title')}</h3>
        <button className="tweaks-close" onClick={onClose}>×</button>
      </div>

      <div className="tweaks-group">
        <label>{t('tweaks.section')}</label>
        <div className="tweaks-section-scroll">
          {(window.NAV_GROUPS || []).map(g => (
            <div key={g.titleKey || g.title} className="tweaks-section-group">
              <div className="tweaks-section-grouptitle">{g.titleKey ? t(g.titleKey) : g.title}</div>
              <div className="tweaks-opts">
                {g.items.map(it => opt('section', it.id, it.key ? t(it.key) : it.label))}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="tweaks-group">
        <label>{t('tweaks.density')}</label>
        <div className="tweaks-opts">
          {opt('density', 'compact')}
          {opt('density', 'comfortable')}
          {opt('density', 'spacious')}
        </div>
      </div>

      <div className="tweaks-group">
        <label>{t('tweaks.chartType')}</label>
        <div className="tweaks-opts">
          {opt('chartType', 'area')}
          {opt('chartType', 'line')}
          {opt('chartType', 'bars')}
        </div>
      </div>

      <div className="tweaks-group">
        <label>{t('tweaks.motion')}</label>
        <div className="tweaks-opts">
          {opt('motion', 'none')}
          {opt('motion', 'subtle')}
          {opt('motion', 'full')}
        </div>
      </div>

      <div className="tweaks-group">
        <label>{t('tweaks.liveSpeed')} · {tweaks.liveSpeed.toFixed(1)}×</label>
        <input
          type="range" min="0.3" max="3" step="0.1"
          value={tweaks.liveSpeed}
          className="tweaks-slider"
          onChange={e => setTweak('liveSpeed', parseFloat(e.target.value))}
        />
      </div>

      <div className="tweaks-group">
        <label>{t('tweaks.accent')}</label>
        <div className="tweaks-swatches">
          {accents.map(a => (
            <button key={a.id}
              className={'tweaks-sw' + (tweaks.accent === a.id ? ' active' : '')}
              style={{ background: a.grad }}
              onClick={() => setTweak('accent', a.id)}
              title={a.id}
            />
          ))}
        </div>
      </div>

      <BackupRestore tweaks={tweaks} setTweak={setTweak}/>
    </div>
  );
}

Object.assign(window, { TweaksPanel });
