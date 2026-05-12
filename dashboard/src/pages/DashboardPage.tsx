import { useNavigate } from 'react-router-dom';
import { useStore } from '../store/useStore.ts';
import { useSignalWs } from '../ws/useSignalWs.ts';
import ConnectionStatus from '../components/ConnectionStatus.tsx';
import SymbolSelector from '../components/SymbolSelector.tsx';
import SignalsBadge from '../components/SignalsBadge.tsx';
import SignalsGrid from '../components/SignalsGrid.tsx';
import ChartWidget from '../components/ChartWidget.tsx';

export default function DashboardPage() {
  const { token, username, activeSymbol, signals, logout, clearSignals } = useStore();
  const navigate = useNavigate();

  // Start the WS connection (reconnects when activeSymbol changes)
  useSignalWs();

  function handleLogout() {
    logout();
    navigate('/login', { replace: true });
  }

  async function handleTestSignal() {
    if (!token) return;
    try {
      await fetch('/api/signals/test', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` },
      });
    } catch {
      // ignore
    }
  }

  return (
    <div className="dashboard-root">
      {/* ── Topbar ─────────────────────────────────────────────── */}
      <header className="topbar">
        <div className="topbar-logo">
          Trading<span>OS</span>
        </div>

        <ConnectionStatus />

        <div className="topbar-spacer" />

        {username && (
          <span className="topbar-user">@{username}</span>
        )}

        <button id="btn-test-signal" className="btn-test" onClick={handleTestSignal} title="Inject a mock signal">
          🧪 Test signal
        </button>

        <button id="btn-logout" className="btn-logout" onClick={handleLogout}>
          Logout
        </button>
      </header>

      {/* ── Body ───────────────────────────────────────────────── */}
      <div className="dashboard-body">
        {/* Sidebar: portfolio symbol list */}
        <SymbolSelector token={token!} />

        {/* Main: signal feed */}
        <main className="main-content">
          <div className="signals-header">
            <h1>
              {activeSymbol ? (
                <>Signals — <span className="sym">{activeSymbol}</span></>
              ) : (
                'Global Signal Feed'
              )}
            </h1>

            <span className="signal-count">
              {activeSymbol 
                ? `${signals.filter(s => s.symbol.toUpperCase() === activeSymbol.toUpperCase()).length} symbol signals`
                : `${signals.length} total signals`
              }
            </span>

            {activeSymbol && (
              <button
                id="btn-clear-signals"
                className="btn-test"
                onClick={clearSignals}
                title="Clear signal history"
                style={{ marginLeft: 'auto' }}
              >
                Clear
              </button>
            )}

            <SignalsBadge />
          </div>

          {activeSymbol && <ChartWidget symbol={activeSymbol} token={token!} />}

          <SignalsGrid />
        </main>
      </div>
    </div>
  );
}
