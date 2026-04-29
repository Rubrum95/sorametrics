/* global window, React */
// ============================================================
// js/minamoto/governance.jsx
// Council + unlocks + Sumeragi consensus telemetry, plus the
// validator-set & roles panel (moved from peers.jsx because the
// validator set is conceptually a Governance concern, not a
// topology one). Pulls from /v1/gov/council/current,
// /v1/gov/unlocks/stats, /v1/sumeragi/telemetry and our
// aggregated /api/minamoto/sumeragi/roles endpoint.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // Generic UI primitives
  // -----------------------------------------------------------
  function Kpi({ label, value, sub, accent }) {
    return (
      <div className="stat-card" style={accent ? { borderTop: '2px solid ' + accent } : null}>
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  function ProgressBar({ value, total, color = '#A062B0' }) {
    const pct = total > 0 ? Math.min(100, (value / total) * 100) : 0;
    return (
      <div style={{ height: 6, borderRadius: 999, background: 'rgba(255,255,255,0.06)', overflow: 'hidden' }}>
        <div style={{ width: pct.toFixed(2) + '%', height: '100%', background: color, transition: 'width 400ms ease' }} />
      </div>
    );
  }

  function RoleBadge({ label, color, title }) {
    return (
      <span title={title} style={{
        display: 'inline-block', padding: '1px 7px', borderRadius: 999,
        fontSize: 9.5, fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase',
        color, border: '1px solid ' + color + '55', background: color + '14',
        marginRight: 4,
      }}>{label}</span>
    );
  }

  function Card({ title, accent = '#A062B0', sub, children }) {
    return (
      <div style={{
        position: 'relative', padding: '20px 22px', borderRadius: 16,
        background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)',
        backdropFilter: 'blur(14px)', overflow: 'hidden',
      }}>
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 2, background: accent, opacity: 0.55 }} />
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 10 }}>
          <h3 style={{ margin: 0, color: '#fafafa', fontSize: 15, fontWeight: 700, letterSpacing: '-0.01em' }}>{title}</h3>
          {sub && <span style={{ color: '#6b7280', fontSize: 11 }}>{sub}</span>}
        </div>
        {children}
      </div>
    );
  }

  function Row({ label, value, hint }) {
    return (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '6px 0', borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
        <span style={{ color: '#9ca3af', fontSize: 12 }}>{label}</span>
        <span className="mono" style={{ color: '#fafafa', fontSize: 13, fontWeight: 600 }}>
          {value}
          {hint && <span style={{ color: '#6b7280', marginLeft: 6, fontWeight: 400 }}>{hint}</span>}
        </span>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Top-level KPI strip — epoch / quorum / leader / validators /
  // view-changes / lane-stake. Pulls roles+council+unlocks at
  // once so the headline matches the body cards.
  // -----------------------------------------------------------
  function GovKpis({ roles, council, unlocks, status }) {
    const t = MN.i18n.useT();
    const c = council || {};
    const u = unlocks || {};
    const s = status && status.sumeragi ? status.sumeragi : {};
    const r = roles || {};
    const members = Array.isArray(c.members) ? c.members.length : 0;
    const leaderResolved = !!r.leader_pubkey;
    return (
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
        <Kpi label={t('gov.kpi.epoch')}     value={r.vrf && r.vrf.epoch != null ? '#' + r.vrf.epoch : (c.epoch != null ? '#' + c.epoch : '—')}
                                            sub={r.epoch_length_blocks ? r.epoch_length_blocks + ' ' + t('roles.kpi.epochLen') : null}
                                            accent="#A062B0" />
        <Kpi label={t('gov.kpi.leader')}    value={leaderResolved ? (r.leader_host + (r.leader_port ? ':' + r.leader_port : '')) : '—'}
                                            sub={leaderResolved ? MN.shortHash(r.leader_pubkey, 10, 6) : t('roles.kpi.leaderUnresolved')} />
        <Kpi label={t('roles.kpi.quorum')}  value={r.quorum != null ? r.quorum + '/' + r.validator_set_len : '—'} sub={t('roles.kpi.quorumSub')} />
        <Kpi label={t('gov.kpi.validators')} value={MN.fmt.int(s.commit_qc_validator_set_len || r.validator_set_len || 0)} sub={t('gov.kpi.validatorsSub')} />
        <Kpi label={t('gov.kpi.viewChanges')} value={MN.fmt.int(r.view_changes_total || 0)}     sub={t('gov.kpi.viewChangesSub')} />
        <Kpi label={t('gov.kpi.expiredLocks')} value={MN.fmt.int(u.expired_locks_now || 0)}      sub={u.height_current != null ? '#' + u.height_current : null} />
      </div>
    );
  }

  // -----------------------------------------------------------
  // Mode banner — surfaces the consensus posture (FALLBACK vs VRF
  // randomized). Iroha 3 starts in fallback mode after genesis: the
  // genesis admin acts as authority and leader rotation uses the
  // zero seed. Once epoch 0's VRF finalises, the seed becomes a
  // hash of the reveals and the rotation is genuinely random per
  // (height, view). This banner explains where we are and what
  // unblocks the next phase.
  // -----------------------------------------------------------
  function ModeBanner({ roles, council }) {
    const t = MN.i18n.useT();
    const isFallback = council && (!Array.isArray(council.members) || council.members.length === 0);
    const vrfFinal = roles && roles.vrf && roles.vrf.finalized;
    const seedZeros = roles && roles.prf_epoch_seed === '0000000000000000000000000000000000000000000000000000000000000000';
    const color = isFallback || !vrfFinal ? '#F59E0B' : '#10B981';
    const phase = isFallback || !vrfFinal ? t('gov.mode.fallback') : t('gov.mode.vrf');
    return (
      <div style={{
        padding: '14px 18px', borderRadius: 12, marginBottom: 14,
        background: color + '0E', border: '1px solid ' + color + '40',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
          <span style={{ width: 8, height: 8, borderRadius: '50%', background: color, boxShadow: '0 0 8px ' + color + '99' }} />
          <span style={{ color, fontSize: 11, fontWeight: 700, letterSpacing: '0.16em', textTransform: 'uppercase' }}>
            {t('gov.mode.label')}: {phase}
          </span>
        </div>
        <div style={{ color: 'var(--fg-2)', fontSize: 12, lineHeight: 1.55 }}>
          {isFallback ? t('gov.mode.fallbackBody') : (vrfFinal ? t('gov.mode.vrfBody') : t('gov.mode.transitionBody'))}
          {seedZeros && (
            <> · <span className="num" style={{ color: '#F59E0B' }}>seed = 0x000…000</span> ({t('gov.mode.genesisSeed')})</>
          )}
        </div>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Epoch progress card — shows where we are in the current epoch.
  // height_into_epoch = prf_height % epoch_length. Sub-line gives
  // commit_deadline_offset and reveal_deadline_offset so users see
  // when the VRF phases open within the epoch.
  // -----------------------------------------------------------
  function EpochProgress({ roles }) {
    const t = MN.i18n.useT();
    if (!roles) return null;
    const epochLen = roles.epoch_length_blocks;
    const heightInEpoch = epochLen && roles.prf_height != null ? (roles.prf_height % epochLen) : null;
    const remaining = epochLen != null && heightInEpoch != null ? (epochLen - heightInEpoch) : null;
    const vrf = roles.vrf || {};
    const phase = vrf.reveals_total > 0 ? 'reveal' : (vrf.commitments_total > 0 ? 'commit' : 'idle');
    const phaseColor = { commit: '#60A5FA', reveal: '#10B981', idle: '#6b7280' }[phase];
    const phaseLabel = { commit: t('gov.epoch.phaseCommit'), reveal: t('gov.epoch.phaseReveal'), idle: t('gov.epoch.phaseIdle') }[phase];

    return (
      <Card title={t('gov.epoch.title')} accent="#60A5FA"
            sub={epochLen != null ? `${epochLen} ${t('roles.kpi.epochLen')}` : null}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 14, alignItems: 'center', marginBottom: 12 }}>
          <div>
            <div className="muted tiny" style={{ marginBottom: 4 }}>
              {heightInEpoch != null && epochLen ? (
                <>{t('gov.epoch.block')} <span className="num" style={{ color: 'var(--fg-0)' }}>{heightInEpoch}</span> / {epochLen}
                {remaining != null && <> · {t('gov.epoch.remaining')} <span className="num">{remaining}</span></>}</>
              ) : '—'}
            </div>
            <ProgressBar value={heightInEpoch || 0} total={epochLen || 1} color="#60A5FA" />
          </div>
          <span style={{
            fontSize: 11, fontWeight: 700, letterSpacing: '0.06em',
            padding: '4px 10px', borderRadius: 999, color: phaseColor,
            background: phaseColor + '14', border: '1px solid ' + phaseColor + '55',
          }}>{phaseLabel}</span>
        </div>
        <Row label={t('gov.epoch.commitDeadline')}
             value={vrf.commit_deadline_offset != null ? '+' + vrf.commit_deadline_offset + ' ' + t('gov.epoch.fromEpochStart') : '—'} />
        <Row label={t('gov.epoch.revealDeadline')}
             value={vrf.reveal_deadline_offset != null ? '+' + vrf.reveal_deadline_offset + ' ' + t('gov.epoch.fromEpochStart') : '—'} />
        <Row label={t('gov.epoch.commits')}  value={(vrf.commitments_total || 0) + ' / ' + (vrf.participants_total || 0)} />
        <Row label={t('gov.epoch.reveals')}  value={(vrf.reveals_total || 0) + ' / ' + (vrf.participants_total || 0)} />
        <Row label={t('roles.vrf.finalized')}
             value={vrf.finalized ? t('roles.vrf.yes') : t('roles.vrf.pending')}
             hint={vrf.finalized ? null : t('gov.epoch.finalisesAtEnd')} />
      </Card>
    );
  }

  // -----------------------------------------------------------
  // Council card
  // -----------------------------------------------------------
  function CouncilCard({ council }) {
    const t = MN.i18n.useT();
    if (!council) return <Card title={t('gov.council')}><div style={{ color: '#9ca3af', fontSize: 12 }}>{t('common.loading')}</div></Card>;
    const members = Array.isArray(council.members) ? council.members : [];
    const alternates = Array.isArray(council.alternates) ? council.alternates : [];
    return (
      <Card title={t('gov.council')} accent="#A062B0" sub={council.derived_by ? `derived: ${council.derived_by}` : null}>
        <Row label={t('gov.epoch')}        value={council.epoch ?? 0} />
        <Row label={t('gov.members')}      value={members.length} />
        <Row label={t('gov.alternates')}   value={alternates.length} />
        <Row label={t('gov.candidates')}   value={council.candidate_count ?? 0} />
        <Row label={t('gov.verified')}     value={council.verified ?? 0} />
        {members.length === 0 && (
          <div style={{ marginTop: 10, padding: 8, borderRadius: 8, background: 'rgba(245,158,11,0.08)', border: '1px solid rgba(245,158,11,0.20)', color: '#F59E0B', fontSize: 11 }}>
            {t('gov.fallbackHint')}
          </div>
        )}
        {members.length > 0 && (
          <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
            <div style={{ fontSize: 10, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 6 }}>{t('gov.members')}</div>
            {members.slice(0, 12).map((m, i) => (
              <div key={i} className="mono" style={{ color: '#C8A0B8', fontSize: 11, padding: '3px 0' }}>{typeof m === 'string' ? MN.shortAccount(m, 12, 8) : JSON.stringify(m).slice(0, 60)}</div>
            ))}
          </div>
        )}
      </Card>
    );
  }

  function UnlocksCard({ unlocks }) {
    const t = MN.i18n.useT();
    if (!unlocks) return <Card title={t('gov.unlocks')}><div style={{ color: '#9ca3af', fontSize: 12 }}>{t('common.loading')}</div></Card>;
    return (
      <Card title={t('gov.unlocks')} accent="#F59E0B">
        <Row label={t('gov.heightNow')}            value={'#' + (unlocks.height_current ?? 0)} />
        <Row label={t('gov.expiredLocksNow')}      value={unlocks.expired_locks_now ?? 0} />
        <Row label={t('gov.referendaWithExpired')} value={unlocks.referenda_with_expired ?? 0} />
        <Row label={t('gov.lastSweep')}            value={unlocks.last_sweep_height != null ? '#' + unlocks.last_sweep_height : '—'} />
      </Card>
    );
  }

  // -----------------------------------------------------------
  // Sumeragi consensus telemetry — leader, validators, VRF, RBC.
  // The "leader_index = 0" row from the previous version was
  // removed because the API field is structurally always 0 (the
  // protocol rotates the topology so the leader sits at position
  // 0 by design); displaying it confused users into thinking the
  // first listed validator is the leader. We now show the
  // resolved leader pubkey + host computed via Blake2b-512 PRF on
  // the canonically sorted roster.
  // -----------------------------------------------------------
  function SumeragiCard({ roles, telemetry, status }) {
    const t = MN.i18n.useT();
    if (!telemetry) return <Card title={t('gov.consensus')}><div style={{ color: '#9ca3af', fontSize: 12 }}>{t('common.loading')}</div></Card>;
    const T = telemetry;
    const S = status || {};
    const sum = S.sumeragi || {};
    const r = roles || {};
    const leaderResolved = !!r.leader_pubkey;
    return (
      <Card title={t('gov.consensus')} accent="#60A5FA" sub={sum.mode_tag || 'iroha2-consensus'}>
        <Row label={t('gov.validatorSet')}    value={sum.commit_qc_validator_set_len ?? '—'} hint={sum.commit_signatures_required != null ? `quorum ${sum.commit_signatures_required}` : ''} />
        <Row label={t('gov.currentLeader')}
             value={leaderResolved ? (r.leader_host + (r.leader_port ? ':' + r.leader_port : '')) : t('roles.kpi.leaderUnresolved')}
             hint={leaderResolved ? MN.shortHash(r.leader_pubkey, 6, 4) : null} />
        <Row label={t('gov.highestQc')}       value={sum.highest_qc_height != null ? '#' + sum.highest_qc_height : '—'} />
        <Row label={t('gov.lockedQc')}        value={sum.locked_qc_height != null ? '#' + sum.locked_qc_height : '—'} />
        <Row label={t('gov.viewChanges')}     value={S.view_changes ?? 0} />
        <Row label={t('gov.queueSize')}       value={S.queue_size ?? 0} />
        <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          <div style={{ fontSize: 10, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 6 }}>VRF</div>
          <Row label={t('gov.vrfEpoch')}        value={T.vrf?.epoch ?? '—'} />
          <Row label={t('gov.vrfFinalized')}    value={T.vrf?.finalized ? '✓' : '✗'} />
          <Row label={t('gov.vrfParticipants')} value={T.vrf?.participants_total ?? 0} />
          <Row label={t('gov.vrfCommits')}      value={T.vrf?.commitments_total ?? 0} />
          <Row label={t('gov.vrfLateReveals')}  value={T.vrf?.late_reveals_total ?? 0} />
        </div>
        <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          <div style={{ fontSize: 10, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 6 }}>RBC (Reliable Broadcast)</div>
          <Row label={t('gov.rbcSessions')}    value={T.rbc_pending?.sessions ?? 0} />
          <Row label={t('gov.rbcChunks')}      value={T.rbc_pending?.chunks ?? 0} />
          <Row label={t('gov.rbcBytes')}       value={T.rbc_pending?.bytes ?? 0} />
          <Row label={t('gov.rbcTtl')}         value={T.rbc_pending?.ttl_ms != null ? T.rbc_pending.ttl_ms + ' ms' : '—'} />
        </div>
        <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          <div style={{ fontSize: 10, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 6 }}>{t('gov.availability')}</div>
          <Row label={t('gov.collectors')}    value={(T.availability?.collectors || []).length} />
          <Row label={t('gov.votesIngested')} value={T.availability?.total_votes_ingested ?? 0} />
        </div>
      </Card>
    );
  }

  // -----------------------------------------------------------
  // LaneStakingPanel — Public Lane staking lifecycle (NX-9). Each
  // validator goes pending → active → (jailed | exiting | exited |
  // slashed). We derive the current state by replaying
  // `RegisterPublicLaneValidator` and `ActivatePublicLaneValidator`
  // ISIs — the only ones currently observed on Minamoto. Future
  // states (jailed/exiting/slashed) will need their own ISI kinds
  // wired in when the operators emit them.
  // -----------------------------------------------------------
  function StatusBadge({ status }) {
    const c = {
      active:              '#10B981',
      pending_activation:  '#F59E0B',
      jailed:              '#EF4444',
      exiting:             '#FB7185',
      exited:              '#6b7280',
      slashed:             '#A855F7',
    }[status] || '#9ca3af';
    const label = {
      active:              'ACTIVE',
      pending_activation:  'PENDING',
      jailed:              'JAILED',
      exiting:             'EXITING',
      exited:              'EXITED',
      slashed:             'SLASHED',
    }[status] || status.toUpperCase();
    return (
      <span style={{
        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
        fontSize: 10, fontWeight: 700, letterSpacing: '0.06em',
        color: c, background: c + '14', border: '1px solid ' + c + '50',
      }}>{label}</span>
    );
  }

  function LaneStakingPanel() {
    const t = MN.i18n.useT();
    const { data } = MN.useFetch('/lane-staking/lifecycle', 60_000);
    if (!data) return null;
    const validators = Array.isArray(data.validators) ? data.validators : [];
    const counts = validators.reduce((acc, v) => { acc[v.status] = (acc[v.status] || 0) + 1; return acc; }, {});

    return (
      <div style={{ marginTop: 24 }}>
        <div className="page-header" style={{ marginBottom: 14 }}>
          <div>
            <h2 className="page-title" style={{ fontSize: 18 }}>{t('staking.title')}</h2>
            <div className="page-sub">{t('staking.subtitle')}</div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 12, marginBottom: 14 }}>
          <Kpi label={t('staking.kpi.totalValidators')} value={MN.fmt.int(validators.length)} accent="#A062B0" />
          <Kpi label={t('staking.kpi.active')}          value={MN.fmt.int(counts.active || 0)} sub={validators.length ? Math.round(((counts.active || 0) / validators.length) * 100) + '%' : null} />
          <Kpi label={t('staking.kpi.pending')}         value={MN.fmt.int(counts.pending_activation || 0)} />
          <Kpi label={t('staking.kpi.jailedExiting')}   value={MN.fmt.int((counts.jailed || 0) + (counts.exiting || 0) + (counts.exited || 0) + (counts.slashed || 0))} />
          <Kpi label={t('staking.kpi.totalEvents')}     value={MN.fmt.int(data.total_events)} sub={t('staking.kpi.totalEventsSub')} />
        </div>

        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                        padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
            <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{t('staking.roster')}</h3>
            <span className="muted tiny">{t('staking.derived')}</span>
          </div>
          <div className="swaps-table-wrap responsive-table">
            <table className="swaps-table">
              <thead>
                <tr>
                  <th>{t('staking.col.pubkey')}</th>
                  <th>{t('staking.col.status')}</th>
                  <th>{t('staking.col.registered')}</th>
                  <th>{t('staking.col.activated')}</th>
                  <th style={{ textAlign: 'right' }}>{t('staking.col.events')}</th>
                </tr>
              </thead>
              <tbody>
                {validators.length === 0 && (
                  <tr><td colSpan={5} style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)' }}>{t('common.empty')}</td></tr>
                )}
                {validators.map(v => (
                  <tr key={v.pubkey}>
                    <td data-label={t('staking.col.pubkey')}>
                      <span className="num" title={v.pubkey} style={{ color: 'var(--accent)', fontSize: 11 }}>
                        {MN.shortHash(v.pubkey, 12, 6)}
                      </span>
                    </td>
                    <td data-label={t('staking.col.status')}>
                      <StatusBadge status={v.status} />
                    </td>
                    <td data-label={t('staking.col.registered')}>
                      {v.registered_block != null ? (
                        <a className="num" href={'#tx/' + v.registered_tx}
                           style={{ color: 'var(--fg-1)', fontSize: 11, textDecoration: 'none' }}>
                          #{v.registered_block} <span className="muted tiny">· {MN.fmt.relative(v.registered_at)}</span>
                        </a>
                      ) : <span className="muted">—</span>}
                    </td>
                    <td data-label={t('staking.col.activated')}>
                      {v.activated_block != null ? (
                        <a className="num" href={'#tx/' + v.activated_tx}
                           style={{ color: '#10B981', fontSize: 11, textDecoration: 'none' }}>
                          #{v.activated_block} <span className="muted tiny">· {MN.fmt.relative(v.activated_at)}</span>
                        </a>
                      ) : <span className="muted">—</span>}
                    </td>
                    <td data-label={t('staking.col.events')} style={{ textAlign: 'right' }}>
                      <span className="num muted" style={{ fontSize: 11 }}>{v.events.length}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="muted tiny" style={{ marginTop: 8 }}>{t('staking.note')}</div>
      </div>
    );
  }

  // -----------------------------------------------------------
  // ValidatorRolesPanel — moved here from peers.jsx. Renders the
  // validator-set roster with per-row roles (Leader / Voter / VRF
  // / RBC), Torii + P2P endpoints, posts and RBC queue depth from
  // Prometheus. The leader row is highlighted by pubkey match
  // resolved empirically via /v1/sumeragi/collectors complement.
  // -----------------------------------------------------------
  function ValidatorRolesPanel({ roles }) {
    const t = MN.i18n.useT();
    if (!roles) return null;
    const roster = Array.isArray(roles.roster) ? roles.roster : [];
    const lanes  = Array.isArray(roles.lanes) ? roles.lanes : [];
    const vrf    = roles.vrf || {};
    const leaderPk = roles.leader_pubkey;
    const leaderResolved = !!leaderPk;

    return (
      <div style={{ marginTop: 24 }}>
        <div className="page-header" style={{ marginBottom: 14 }}>
          <div>
            <h2 className="page-title" style={{ fontSize: 18 }}>{t('roles.title')}</h2>
            <div className="page-sub">{roles.mode_tag || 'iroha2-consensus'} · {t('roles.subtitle')}</div>
          </div>
        </div>

        {leaderResolved && roles.leader_resolution && (
          <div style={{
            padding: '10px 14px', borderRadius: 10, marginBottom: 14,
            background: 'rgba(16,185,129,0.06)', border: '1px solid rgba(16,185,129,0.25)',
            color: 'var(--fg-2)', fontSize: 11, lineHeight: 1.6,
          }}>
            <span style={{ color: '#10B981', fontWeight: 700 }}>{t('roles.resolution.label')}: </span>
            {t('roles.resolution.empiricalMethod')} · <span className="num">/v1/sumeragi/collectors</span>
            {roles.leader_resolution.prf_cross_check && (
              <>
                {' · '}
                <span style={{ color: roles.leader_resolution.prf_cross_check.matches_empirical ? '#10B981' : '#F59E0B' }}>
                  {roles.leader_resolution.prf_cross_check.matches_empirical
                    ? t('roles.resolution.prfMatches')
                    : t('roles.resolution.prfDiverges')}
                </span>
                {!roles.leader_resolution.prf_cross_check.matches_empirical && (
                  <div className="muted tiny" style={{ marginTop: 4, fontStyle: 'italic' }}>
                    {t('roles.resolution.prfDivergesNote')}
                  </div>
                )}
              </>
            )}
          </div>
        )}

        <div className="card" style={{ padding: 0, overflow: 'hidden', marginBottom: 14 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                        padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
            <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{t('roles.roster')}</h3>
            <span className="tag">{roster.length}</span>
          </div>
          <div className="swaps-table-wrap responsive-table">
            <table className="swaps-table">
              <thead>
                <tr>
                  <th>{t('roles.col.pubkey')}</th>
                  <th>{t('roles.col.torii')}</th>
                  <th>{t('roles.col.p2p')}</th>
                  <th>{t('roles.col.roles')}</th>
                  <th style={{ textAlign: 'right' }}>{t('roles.col.posts')}</th>
                  <th style={{ textAlign: 'right' }}>{t('roles.col.rbcQueue')}</th>
                  <th style={{ textAlign: 'center' }}>{t('roles.col.status')}</th>
                </tr>
              </thead>
              <tbody>
                {roster.map(r => {
                  const isLeader = leaderResolved && r.pubkey === leaderPk;
                  return (
                    <tr key={r.pubkey} style={isLeader ? { background: 'rgba(245,158,11,0.06)' } : null}>
                      <td data-label={t('roles.col.pubkey')}>
                        <span className="num" title={r.pubkey} style={{ color: 'var(--accent)', fontSize: 11 }}>
                          {MN.shortHash(r.pubkey, 12, 6)}
                        </span>
                        {r.ownership === 'self_disclosed' && (
                          <span className="muted tiny" style={{ marginLeft: 6 }} title={t('roles.ownership.selfTip')}>✓</span>
                        )}
                        {r.ownership === 'deduced' && (
                          <span className="muted tiny" style={{ marginLeft: 6 }} title={t('roles.ownership.deducedTip')}>↗</span>
                        )}
                      </td>
                      <td data-label={t('roles.col.torii')}>
                        {r.torii_host ? (
                          <a href={r.torii_url} target="_blank" rel="noopener" className="num"
                             style={{ color: 'var(--fg-0)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
                            {r.torii_host}:{r.torii_port}
                          </a>
                        ) : <span className="muted">—</span>}
                      </td>
                      <td data-label={t('roles.col.p2p')}>
                        {r.p2p_host ? (
                          <span className="num" style={{ color: 'var(--fg-1)', fontSize: 11 }}>
                            {r.p2p_host}:{r.p2p_port}
                          </span>
                        ) : <span className="muted">—</span>}
                      </td>
                      <td data-label={t('roles.col.roles')}>
                        {isLeader && <RoleBadge label={t('roles.badge.leader')} color="#F59E0B" title={t('roles.badge.leaderTip')} />}
                        <RoleBadge label={t('roles.badge.voter')} color="#10B981" title={t('roles.badge.voterTip')} />
                        <RoleBadge label={t('roles.badge.vrf')}   color="#60A5FA" title={t('roles.badge.vrfTip')} />
                        <RoleBadge label={t('roles.badge.rbc')}   color="#A062B0" title={t('roles.badge.rbcTip')} />
                      </td>
                      <td data-label={t('roles.col.posts')} style={{ textAlign: 'right' }}>
                        <span className="num" style={{ color: 'var(--fg-1)', fontSize: 12 }}>{MN.fmt.int(r.post_total)}</span>
                      </td>
                      <td data-label={t('roles.col.rbcQueue')} style={{ textAlign: 'right' }}>
                        <span className="num" style={{ color: r.rbc_queue > 0 ? '#F59E0B' : 'var(--fg-2)', fontSize: 12 }}>{MN.fmt.int(r.rbc_queue)}</span>
                      </td>
                      <td data-label={t('roles.col.status')} style={{ textAlign: 'center' }}>
                        {r.connected
                          ? <span className="status-pill ok" title="online">✓</span>
                          : <span className="status-pill err" title="offline">✗</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: 14 }}>
          <div className="card" style={{ padding: '16px 18px' }}>
            <div className="stat-label" style={{ marginBottom: 10 }}>{t('roles.lanes')}</div>
            {lanes.length === 0 ? (
              <div className="muted tiny">{t('common.empty')}</div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {lanes.map(l => (
                  <div key={l.lane} style={{
                    display: 'grid', gridTemplateColumns: '60px 1fr 80px 80px',
                    alignItems: 'center', gap: 10, padding: '8px 0',
                    borderBottom: '1px solid var(--border)',
                  }}>
                    <span style={{
                      display: 'inline-block', padding: '2px 8px', borderRadius: 6,
                      background: 'rgba(160,98,176,0.14)', color: '#C8A0B8',
                      border: '1px solid rgba(160,98,176,0.30)',
                      fontSize: 11, fontWeight: 700, letterSpacing: '0.04em',
                      textAlign: 'center',
                    }}>L{l.lane}</span>
                    <ProgressBar value={l.active} total={roles.validator_set_len || (l.active + l.pending)} />
                    <span className="num" style={{ color: '#10B981', fontSize: 12, textAlign: 'right' }}>{l.active} {t('roles.active')}</span>
                    <span className="num muted" style={{ fontSize: 11, textAlign: 'right' }}>{l.pending} {t('roles.pending')}</span>
                  </div>
                ))}
              </div>
            )}
            <div className="muted tiny" style={{ marginTop: 8 }}>{t('roles.lanesHint')}</div>
          </div>

          <div className="card" style={{ padding: '16px 18px' }}>
            <div className="stat-label" style={{ marginBottom: 10 }}>{t('roles.vrf')}</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span className="muted tiny">{t('roles.vrf.commits')}</span>
                  <span className="num" style={{ color: 'var(--fg-0)', fontSize: 12, fontWeight: 700 }}>
                    {vrf.commitments_total || 0} / {vrf.participants_total || 0}
                  </span>
                </div>
                <ProgressBar value={vrf.commitments_total || 0} total={vrf.participants_total || 1} color="#60A5FA" />
              </div>
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span className="muted tiny">{t('roles.vrf.reveals')}</span>
                  <span className="num" style={{ color: 'var(--fg-0)', fontSize: 12, fontWeight: 700 }}>
                    {vrf.reveals_total || 0} / {vrf.participants_total || 0}
                  </span>
                </div>
                <ProgressBar value={vrf.reveals_total || 0} total={vrf.participants_total || 1} color="#10B981" />
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', paddingTop: 6, borderTop: '1px solid var(--border)' }}>
                <span className="muted tiny">{t('roles.vrf.finalized')}</span>
                <span style={{
                  fontSize: 11, fontWeight: 700, letterSpacing: '0.06em',
                  padding: '2px 8px', borderRadius: 999,
                  color: vrf.finalized ? '#10B981' : '#F59E0B',
                  background: vrf.finalized ? 'rgba(16,185,129,0.10)' : 'rgba(245,158,11,0.10)',
                  border: '1px solid ' + (vrf.finalized ? 'rgba(16,185,129,0.30)' : 'rgba(245,158,11,0.30)'),
                }}>{vrf.finalized ? t('roles.vrf.yes') : t('roles.vrf.pending')}</span>
              </div>
              <div className="muted tiny">{t('roles.vrf.hint')}</div>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
                <span className="muted">{t('roles.vrf.lateReveals')}</span>
                <span className="num" style={{ color: vrf.late_reveals_total > 0 ? '#F59E0B' : 'var(--fg-2)' }}>{vrf.late_reveals_total || 0}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Top-level Governance section
  // -----------------------------------------------------------
  function Governance() {
    const t = MN.i18n.useT();
    const rolesQ    = MN.useFetch('/sumeragi/roles', 30_000);
    const councilQ  = MN.useFetch('/gov/council', 60_000);
    const unlocksQ  = MN.useFetch('/gov/unlocks', 60_000);
    const telemetryQ = MN.useFetch('/telemetry/sumeragi', 30_000);
    const statusQ   = MN.useFetch('/status', 30_000);

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('gov.title')}</h1>
            <div className="page-sub">{t('gov.subtitle')}</div>
          </div>
        </div>

        <GovKpis    roles={rolesQ.data} council={councilQ.data} unlocks={unlocksQ.data} status={statusQ.data} />
        <ModeBanner roles={rolesQ.data} council={councilQ.data} />

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: 16 }}>
          <EpochProgress roles={rolesQ.data} />
          <CouncilCard   council={councilQ.data} />
          <UnlocksCard   unlocks={unlocksQ.data} />
          <SumeragiCard  roles={rolesQ.data} telemetry={telemetryQ.data} status={statusQ.data} />
        </div>

        <ValidatorRolesPanel roles={rolesQ.data} />
        <LaneStakingPanel />
      </section>
    );
  }

  MN.Governance = Governance;
})(window.MN);
