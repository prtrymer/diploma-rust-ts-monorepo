import { useStore } from '../store/useStore.ts';

export default function SignalsBadge() {
  const allSignals = useStore((s) => s.signals);
  const activeSymbol = useStore((s) => s.activeSymbol);

  const signal = allSignals.find(s => 
    !activeSymbol || s.symbol.toUpperCase() === activeSymbol.toUpperCase()
  );

  if (!signal) return null;

  const meta = signal.metadata
    ? (() => {
        try {
          const obj = JSON.parse(signal.metadata);
          return Object.entries(obj)
            .map(([k, v]) => `${k}:${v}`)
            .join(' · ');
        } catch {
          return signal.metadata;
        }
      })()
    : null;

  return (
    <div className={`signal-badge ${signal.direction.toLowerCase()}`}>
      <div className="badge-content">
        <div className="badge-row">
          <span className="badge-symbol">{signal.symbol}</span>
          <span className="badge-direction">{signal.direction}</span>
        </div>
        <div className="badge-row secondary">
          <span>{(signal.strength * 100).toFixed(2)}% confidence</span>
          {meta && <span className="badge-meta">{meta}</span>}
        </div>
      </div>
      <div className="badge-time">
        {new Date(signal.timestamp).toLocaleTimeString()}
      </div>
    </div>
  );
}
