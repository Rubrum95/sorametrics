/* global window, React */
// ============================================================
// js/minamoto/lanes.jsx
// Per-lane activity: ZK Halo2, AMX cross-dataspace, IVM (smart
// contracts), Confidential gas. All data lives in /metrics so we
// just consume the parsed Prometheus snapshot. Refreshed every 30 s.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------
  function bucketByName(samples, prefix) {
    const out = new Map();
    for (const s of samples) {
      if (!s.name.startsWith(prefix)) continue;
      if (!out.has(s.name)) out.set(s.name, []);
      out.get(s.name).push(s);
    }
    return out;
  }

  function scalar(samples, name) {
    const m = samples.find(s => s.name === name && Object.keys(s.labels).length === 0);
    return m ? m.value : null;
  }

  // For histogram-like metrics with a "le" bucket label, returns the median
  // approximation as bucketBoundary where cumulative >= 50%. Best-effort.
  function p50FromHistogram(samples, baseName) {
    const buckets = samples
      .filter(s => s.name === baseName + '_bucket' && s.labels && s.labels.le)
      .map(s => ({ le: parseFloat(s.labels.le), v: s.value }))
      .filter(b => Number.isFinite(b.le))
      .sort((a, b) => a.le - b.le);
    if (buckets.length === 0) return null;
    const total = buckets[buckets.length - 1].v;
    if (!total) return null;
    const target = total * 0.5;
    for (const b of buckets) if (b.v >= target) return b.le;
    return null;
  }

  function avgFromCount(samples, baseName) {
    const sum = scalar(samples, baseName + '_sum');
    const cnt = scalar(samples, baseName + '_count');
    if (sum == null || !cnt) return null;
    return sum / cnt;
  }

  // Curve / backend label decoders (per the HELP comments in /metrics)
  const HALO2_CURVE = { 0: 'Pallas', 1: 'Pasta', 2: 'Goldilocks', 3: 'Bn254' };
  const HALO2_BACKEND = { 0: 'IPA', 1: 'Unsupported' };

  // -----------------------------------------------------------
  // Tiny presentation atoms
  // -----------------------------------------------------------
  function Card({ title, accent = '#A062B0', children, sub }) {
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

  function StatusPill({ on, label }) {
    return (
      <span style={{
        display: 'inline-flex', alignItems: 'center', gap: 6,
        padding: '3px 10px', borderRadius: 999, fontSize: 11, fontWeight: 700, letterSpacing: '0.06em',
        background: on ? 'rgba(16,185,129,0.14)' : 'rgba(107,114,128,0.14)',
        color: on ? '#10B981' : '#9ca3af',
        border: '1px solid ' + (on ? 'rgba(16,185,129,0.30)' : 'rgba(255,255,255,0.10)'),
      }}>
        <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'currentColor' }} />
        {label}
      </span>
    );
  }

  // -----------------------------------------------------------
  // Per-lane sections
  // -----------------------------------------------------------

  function ZkLane({ samples }) {
    const enabled = scalar(samples, 'iroha_zk_halo2_enabled') === 1;
    const curveId = scalar(samples, 'iroha_zk_halo2_curve_id');
    const backendId = scalar(samples, 'iroha_zk_halo2_backend_id');
    const maxK = scalar(samples, 'iroha_zk_halo2_max_k');
    const budget = scalar(samples, 'iroha_zk_halo2_verifier_budget_ms');
    const maxBatch = scalar(samples, 'iroha_zk_halo2_verifier_max_batch');
    const queueCap = scalar(samples, 'iroha_zk_halo2_verifier_queue_cap');
    const workers = scalar(samples, 'iroha_zk_halo2_verifier_worker_threads');
    const pendingDepth = scalar(samples, 'iroha_zk_lane_pending_depth');
    const ringDepth = scalar(samples, 'iroha_zk_lane_retry_ring_depth');
    const enqTimeout = scalar(samples, 'iroha_zk_lane_enqueue_timeout_total');
    const enqWait = scalar(samples, 'iroha_zk_lane_enqueue_wait_total');
    const retryEnq = scalar(samples, 'iroha_zk_lane_retry_enqueued_total');
    const retryExh = scalar(samples, 'iroha_zk_lane_retry_exhausted_total');
    const retryReplay = scalar(samples, 'iroha_zk_lane_retry_replayed_total');

    return (
      <Card title="ZK lane (Halo2)" accent="#A062B0" sub={enabled ? 'verification active' : 'disabled'}>
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <StatusPill on={enabled} label={enabled ? 'Halo2 enabled' : 'Halo2 disabled'} />
          {curveId != null && <StatusPill on label={'curve: ' + (HALO2_CURVE[curveId] || curveId)} />}
          {backendId != null && <StatusPill on label={'backend: ' + (HALO2_BACKEND[backendId] || backendId)} />}
        </div>
        <Row label="Max k (circuit exponent)" value={maxK ?? '—'} />
        <Row label="Verifier budget" value={budget != null ? budget + ' ms' : '—'} />
        <Row label="Max batch / queue cap" value={maxBatch != null ? `${maxBatch} / ${queueCap}` : '—'} />
        <Row label="Worker threads" value={workers ?? '—'} />
        <div style={{ marginTop: 12, paddingTop: 10, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          <div style={{ fontSize: 11, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 6 }}>Queue activity</div>
          <Row label="Pending depth"   value={pendingDepth ?? 0} />
          <Row label="Retry ring depth" value={ringDepth ?? 0} />
          <Row label="Enqueue timeouts" value={enqTimeout ?? 0} hint="cumulative" />
          <Row label="Enqueue waits"    value={enqWait ?? 0} hint="cumulative" />
          <Row label="Retries enq / replay / exhaust" value={`${retryEnq ?? 0} / ${retryReplay ?? 0} / ${retryExh ?? 0}`} />
        </div>
      </Card>
    );
  }

  function AmxLanes({ samples }) {
    // iroha_amx_commit_ms_bucket{le="...",lane="..."}
    // iroha_amx_prepare_ms_bucket{le="...",lane="..."}
    const lanes = new Set();
    for (const s of samples) {
      if (s.name.startsWith('iroha_amx_') && s.labels && s.labels.lane) {
        lanes.add(s.labels.lane);
      }
    }
    const laneIds = [...lanes].sort();

    function laneStats(lane) {
      const filt = samples.filter(s => s.labels && s.labels.lane === lane);
      const commitSum = filt.find(s => s.name === 'iroha_amx_commit_ms_sum');
      const commitCnt = filt.find(s => s.name === 'iroha_amx_commit_ms_count');
      const prepareSum = filt.find(s => s.name === 'iroha_amx_prepare_ms_sum');
      const prepareCnt = filt.find(s => s.name === 'iroha_amx_prepare_ms_count');
      return {
        commit_avg_ms: commitSum && commitCnt && commitCnt.value > 0 ? (commitSum.value / commitCnt.value) : null,
        commit_count: commitCnt ? commitCnt.value : 0,
        prepare_avg_ms: prepareSum && prepareCnt && prepareCnt.value > 0 ? (prepareSum.value / prepareCnt.value) : null,
        prepare_count: prepareCnt ? prepareCnt.value : 0,
      };
    }

    return (
      <Card title="AMX cross-dataspace lanes" accent="#60A5FA" sub={`${laneIds.length} lane(s) observed`}>
        {laneIds.length === 0 && <div style={{ color: '#6b7280', fontSize: 12 }}>No AMX lane activity yet.</div>}
        {laneIds.map(lane => {
          const s = laneStats(lane);
          return (
            <div key={lane} style={{ padding: '10px 0', borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 6 }}>
                <span className="mono" style={{ color: '#fafafa', fontSize: 13, fontWeight: 600 }}>lane: {lane}</span>
                <span style={{ color: '#6b7280', fontSize: 11 }}>commits: {s.commit_count}</span>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, fontSize: 12 }}>
                <div>
                  <span style={{ color: '#9ca3af' }}>prepare avg: </span>
                  <span className="mono" style={{ color: '#fafafa' }}>{s.prepare_avg_ms != null ? s.prepare_avg_ms.toFixed(2) + ' ms' : '—'}</span>
                </div>
                <div>
                  <span style={{ color: '#9ca3af' }}>commit avg: </span>
                  <span className="mono" style={{ color: '#fafafa' }}>{s.commit_avg_ms != null ? s.commit_avg_ms.toFixed(2) + ' ms' : '—'}</span>
                </div>
              </div>
            </div>
          );
        })}
      </Card>
    );
  }

  function IvmLane({ samples }) {
    const lanes = new Set();
    for (const s of samples) {
      if (s.name.startsWith('iroha_ivm_exec_ms') && s.labels && s.labels.lane) lanes.add(s.labels.lane);
    }
    const laneIds = [...lanes].sort();

    const cacheHits = scalar(samples, 'ivm_cache_hits');
    const cacheMisses = scalar(samples, 'ivm_cache_misses');
    const cacheEvict = scalar(samples, 'ivm_cache_evictions');
    const decodedOps = scalar(samples, 'ivm_cache_decoded_ops_total');
    const decodedStreams = scalar(samples, 'ivm_cache_decoded_streams');
    const decodeFails = scalar(samples, 'ivm_cache_decode_failures');

    const total = (cacheHits || 0) + (cacheMisses || 0);
    const hitRate = total > 0 ? (((cacheHits || 0) / total) * 100).toFixed(1) + '%' : '—';

    function laneStats(lane) {
      const filt = samples.filter(s => s.labels && s.labels.lane === lane);
      const sum = filt.find(s => s.name === 'iroha_ivm_exec_ms_sum');
      const cnt = filt.find(s => s.name === 'iroha_ivm_exec_ms_count');
      return {
        avg: sum && cnt && cnt.value > 0 ? sum.value / cnt.value : null,
        count: cnt ? cnt.value : 0,
      };
    }

    return (
      <Card title="IVM lane (smart contracts)" accent="#10B981" sub={`${laneIds.length} lane(s) observed`}>
        <Row label="Cache hit rate" value={hitRate} hint={`${cacheHits ?? 0} hits · ${cacheMisses ?? 0} misses · ${cacheEvict ?? 0} evicted`} />
        <Row label="Pre-decoded ops / streams / fails" value={`${decodedOps ?? 0} / ${decodedStreams ?? 0} / ${decodeFails ?? 0}`} />
        {laneIds.length > 0 && (
          <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
            <div style={{ fontSize: 11, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 6 }}>Execution per lane</div>
            {laneIds.map(lane => {
              const s = laneStats(lane);
              return (
                <Row key={lane} label={'lane: ' + lane}
                  value={s.avg != null ? s.avg.toFixed(2) + ' ms' : '—'}
                  hint={`${s.count} exec`} />
              );
            })}
          </div>
        )}
      </Card>
    );
  }

  function ConfidentialGas({ samples }) {
    const blockUsed = scalar(samples, 'iroha_confidential_gas_block_used');
    const total = scalar(samples, 'iroha_confidential_gas_total');
    const txUsed = scalar(samples, 'iroha_confidential_gas_tx_used');
    const baseVerify = scalar(samples, 'iroha_confidential_gas_base_verify');
    const perProofByte = scalar(samples, 'iroha_confidential_gas_per_proof_byte');
    const perCommitment = scalar(samples, 'iroha_confidential_gas_per_commitment');
    const perNullifier = scalar(samples, 'iroha_confidential_gas_per_nullifier');
    const perPublicInput = scalar(samples, 'iroha_confidential_gas_per_public_input');

    return (
      <Card title="Confidential gas (PvP / private CBDC)" accent="#EC4899" sub="ZK transactions">
        <Row label="Used in latest block" value={blockUsed ?? 0} />
        <Row label="Used in latest tx" value={txUsed ?? 0} />
        <Row label="Cumulative since boot" value={total ?? 0} />
        <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          <div style={{ fontSize: 11, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 6 }}>Cost schedule</div>
          <Row label="Base verify"        value={baseVerify ?? '—'} />
          <Row label="Per proof byte"     value={perProofByte ?? '—'} />
          <Row label="Per commitment"     value={perCommitment ?? '—'} />
          <Row label="Per nullifier"      value={perNullifier ?? '—'} />
          <Row label="Per public input"   value={perPublicInput ?? '—'} />
        </div>
      </Card>
    );
  }

  function DaQuorum({ samples }) {
    const ratio = scalar(samples, 'iroha_da_quorum_ratio');
    const slot = scalar(samples, 'iroha_slot_duration_ms_latest');
    return (
      <Card title="Data Availability + Slot SLO" accent="#F59E0B" sub="finality health">
        <Row label="DA quorum ratio (rolling)" value={ratio != null ? (ratio * 100).toFixed(1) + '%' : '—'} hint="0–100" />
        <Row label="Latest slot duration"      value={slot != null ? slot.toFixed(0) + ' ms' : '—'} hint="NX-18 SLO" />
      </Card>
    );
  }

  // -----------------------------------------------------------
  // Top section
  // -----------------------------------------------------------
  function Lanes() {
    const t = MN.i18n.useT();
    const { data, error, loading } = MN.useFetch('/prometheus/parsed', 30_000);
    const samples = data && data.samples ? data.samples : [];

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">Lanes</h1>
            <div className="page-sub">
              Per-lane activity across the Iroha 3 pipeline: ZK proof verification, AMX cross-dataspace settlement,
              IVM smart-contract execution, confidential gas, and data availability. Refreshed every 30 s from Torii /metrics.
            </div>
          </div>
        </div>

        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}
        {loading && samples.length === 0 && (
          <div style={{ padding: 32, textAlign: 'center', color: '#9ca3af' }}>{t('common.loading')}</div>
        )}
        {samples.length > 0 && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: 16 }}>
            <ZkLane samples={samples} />
            <AmxLanes samples={samples} />
            <IvmLane samples={samples} />
            <ConfidentialGas samples={samples} />
            <DaQuorum samples={samples} />
          </div>
        )}
      </section>
    );
  }

  MN.Lanes = Lanes;
})(window.MN);
