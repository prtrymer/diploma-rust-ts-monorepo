import { useState, useRef, useEffect, useCallback } from 'react';
import { useStore } from '../store/useStore.ts';

interface Props {
  token: string;
}

interface SearchResult {
  symbol: string;
  name: string;
  type: string;
  exchange: string;
}

export default function SymbolSelector({ token }: Props) {
  const { symbols, activeSymbol, setActiveSymbol, addSymbolToPortfolio, removeSymbolFromPortfolio, setSymbols } =
    useStore();
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [highlightIdx, setHighlightIdx] = useState(-1);
  const [searching, setSearching] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Debounced search
  const searchSymbols = useCallback(
    (query: string) => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      if (query.trim().length === 0) {
        setResults([]);
        setShowDropdown(false);
        return;
      }
      debounceRef.current = setTimeout(async () => {
        setSearching(true);
        try {
          const res = await fetch(`/api/symbols/search?q=${encodeURIComponent(query.trim())}`, {
            headers: { Authorization: `Bearer ${token}` },
          });
          if (res.ok) {
            const data = await res.json();
            const filtered = (data.results ?? []).filter(
              (r: SearchResult) => !symbols.includes(r.symbol)
            );
            setResults(filtered);
            setShowDropdown(filtered.length > 0);
            setHighlightIdx(-1);
          }
        } catch {
          // silently fail
        } finally {
          setSearching(false);
        }
      }, 250);
    },
    [token, symbols]
  );

  function handleInputChange(e: React.ChangeEvent<HTMLInputElement>) {
    const val = e.target.value.toUpperCase();
    setInput(val);
    searchSymbols(val);
  }

  // Close dropdown on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node) &&
          inputRef.current && !inputRef.current.contains(e.target as Node)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  async function addSymbol(sym: string) {
    setLoading(true);
    setShowDropdown(false);
    setInput('');
    setResults([]);
    try {
      const res = await fetch('/api/symbols', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ symbol: sym }),
      });
      if (res.ok || res.status === 409) {
        addSymbolToPortfolio(sym);
        // Refresh from server to stay in sync
        const listRes = await fetch('/api/symbols', {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (listRes.ok) {
          const data = await listRes.json();
          setSymbols(data.symbols ?? []);
        }
      }
    } catch {
      // ignore network errors silently
    } finally {
      setLoading(false);
    }
  }

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault();
    const sym = input.trim().toUpperCase();
    if (!sym) return;
    await addSymbol(sym);
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (!showDropdown || results.length === 0) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setHighlightIdx((prev) => (prev < results.length - 1 ? prev + 1 : 0));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setHighlightIdx((prev) => (prev > 0 ? prev - 1 : results.length - 1));
    } else if (e.key === 'Enter' && highlightIdx >= 0) {
      e.preventDefault();
      addSymbol(results[highlightIdx].symbol);
    } else if (e.key === 'Escape') {
      setShowDropdown(false);
    }
  }

  // Scroll highlighted item into view
  useEffect(() => {
    if (highlightIdx >= 0 && dropdownRef.current) {
      const items = dropdownRef.current.querySelectorAll('.search-result-item');
      items[highlightIdx]?.scrollIntoView({ block: 'nearest' });
    }
  }, [highlightIdx]);

  async function handleRemove(sym: string) {
    removeSymbolFromPortfolio(sym);
    try {
      await fetch('/api/symbols', {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ symbol: sym }),
      });
    } catch {
      // best effort
    }
  }

  return (
    <div className="sidebar">
      <div className="sidebar-header">Portfolio</div>
      <div className="sidebar-symbols">
        <div
          className={`symbol-item${!activeSymbol ? ' active' : ''}`}
          onClick={() => setActiveSymbol('')}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => e.key === 'Enter' && setActiveSymbol('')}
          title="View all signals"
          style={{ marginBottom: '0.75rem', borderBottom: '1px solid var(--border-color)', borderRadius: 0, paddingBottom: '0.75rem' }}
        >
          <span className="symbol-dot" style={{ background: 'var(--accent-blue)', boxShadow: '0 0 8px var(--accent-blue)' }} />
          <span style={{ flex: 1, fontWeight: 600 }}>Global Feed</span>
        </div>

        {symbols.length === 0 && !activeSymbol && (
          <div style={{ padding: '0.5rem 0.75rem', color: 'var(--text-muted)', fontSize: '0.82rem' }}>
            Add symbols below to filter
          </div>
        )}
        {symbols.map((sym) => (
          <div
            key={sym}
            className={`symbol-item${activeSymbol === sym ? ' active' : ''}`}
            onClick={() => setActiveSymbol(sym)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => e.key === 'Enter' && setActiveSymbol(sym)}
            title={`View signals for ${sym}`}
          >
            <span className="symbol-dot" />
            <span style={{ flex: 1, fontFamily: 'var(--font-mono)' }}>{sym}</span>
            <button
              onClick={(e) => {
                e.stopPropagation();
                handleRemove(sym);
              }}
              title={`Remove ${sym}`}
              style={{
                background: 'none',
                border: 'none',
                color: 'var(--text-muted)',
                cursor: 'pointer',
                fontSize: '0.9rem',
                lineHeight: 1,
                padding: '0 2px',
                opacity: 1,
              }}
            >
              ×
            </button>
          </div>
        ))}
      </div>
      <div className="sidebar-add">
        <div className="add-symbol-wrap">
          <form className="add-symbol-form" onSubmit={handleAdd}>
            <input
              ref={inputRef}
              className="add-symbol-input"
              type="text"
              placeholder="Search symbols…"
              value={input}
              onChange={handleInputChange}
              onKeyDown={handleKeyDown}
              onFocus={() => {
                if (results.length > 0) setShowDropdown(true);
              }}
              maxLength={20}
              disabled={loading}
              aria-label="Search and add symbol"
              autoComplete="off"
            />
            <button className="btn-add" type="submit" disabled={loading || !input.trim()} aria-label="Add">
              +
            </button>
          </form>

          {showDropdown && results.length > 0 && (
            <div className="search-dropdown" ref={dropdownRef}>
              {results.map((r, i) => (
                <div
                  key={r.symbol}
                  className={`search-result-item${i === highlightIdx ? ' highlighted' : ''}`}
                  onMouseDown={(e) => {
                    e.preventDefault();
                    addSymbol(r.symbol);
                  }}
                  onMouseEnter={() => setHighlightIdx(i)}
                >
                  <div className="search-result-top">
                    <span className="search-result-symbol">{r.symbol}</span>
                    <span className="search-result-exchange">{r.exchange}</span>
                  </div>
                  <div className="search-result-name">{r.name}</div>
                </div>
              ))}
            </div>
          )}

          {searching && (
            <div className="search-loading">
              <span className="search-spinner" />
              Searching…
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
