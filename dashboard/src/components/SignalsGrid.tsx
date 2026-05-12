import { useMemo, useCallback } from 'react';
import DataEditor, {
  type GridColumn,
  type GridCell,
  GridCellKind,
  type Theme,
} from '@glideapps/glide-data-grid';
import '@glideapps/glide-data-grid/dist/index.css';
import { useStore } from '../store/useStore.ts';
import type { SignalEvent } from '../types';

// ─── Column definitions ───────────────────────────────────────────────────────

const COLUMNS: GridColumn[] = [
  { title: 'Time',     width: 90,  id: 'timestamp' },
  { title: 'Dir',      width: 70,  id: 'direction' },
  { title: 'Symbol',   width: 90,  id: 'symbol' },
  { title: 'Strength', width: 90,  id: 'strength' },
  { title: 'Strategy', width: 160, id: 'strategy_name', grow: 1 },
  { title: 'Metadata', width: 260, id: 'metadata', grow: 2 },
];

// ─── Glide theme override (dark) ─────────────────────────────────────────────

const DARK_THEME: Partial<Theme> = {
  accentColor: '#58a6ff',
  accentLight: 'rgba(88,166,255,0.12)',
  textDark: '#e6edf3',
  textMedium: '#8b949e',
  textLight: '#484f58',
  textBubble: '#e6edf3',
  bgIconHeader: '#1c2330',
  fgIconHeader: '#8b949e',
  textHeader: '#8b949e',
  textHeaderSelected: '#e6edf3',
  bgCell: '#0d1117',
  bgCellMedium: '#10161f',
  bgHeader: '#161b22',
  bgHeaderHasFocus: '#1c2330',
  bgHeaderHovered: '#1c2330',
  bgBubble: '#1c2330',
  bgBubbleSelected: '#21262d',
  bgSearchResult: 'rgba(88,166,255,0.12)',
  borderColor: '#21262d',
  drilldownBorder: 'rgba(88,166,255,0.4)',
  linkColor: '#58a6ff',
  cellHorizontalPadding: 10,
  cellVerticalPadding: 4,
  headerFontStyle: '600 12px',
  baseFontStyle: '13px',
  fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
  editorFontSize: '13px',
  lineHeight: 1.5,
  roundingRadius: 4,
};

// ─── Direction colour helper ──────────────────────────────────────────────────

function directionColour(dir: string): string {
  if (dir === 'Long')  return '#3fb950';
  if (dir === 'Short') return '#f85149';
  return '#58a6ff';
}

function formatTs(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString('en-GB', { hour12: false });
  } catch {
    return iso;
  }
}

// ─── Component ────────────────────────────────────────────────────────────────

export default function SignalsGrid() {
  const allSignals = useStore((s) => s.signals);
  const activeSymbol = useStore((s) => s.activeSymbol);

  const signals = useMemo(() => {
    if (!activeSymbol) return allSignals;
    const target = activeSymbol.toUpperCase();
    return allSignals.filter((s) => s.symbol.toUpperCase() === target);
  }, [allSignals, activeSymbol]);

  const getCellContent = useCallback(
    ([col, row]: readonly [number, number]): GridCell => {
      const signal: SignalEvent | undefined = signals[row];
      if (!signal) {
        return { kind: GridCellKind.Text, data: '', displayData: '', allowOverlay: false };
      }

      switch (col) {
        case 0: // Time
          return {
            kind: GridCellKind.Text,
            data: formatTs(signal.timestamp),
            displayData: formatTs(signal.timestamp),
            allowOverlay: false,
            themeOverride: { textDark: '#8b949e' },
          };

        case 1: // Direction
          return {
            kind: GridCellKind.Text,
            data: signal.direction,
            displayData: signal.direction.toUpperCase(),
            allowOverlay: false,
            themeOverride: { textDark: directionColour(signal.direction) },
          };

        case 2: // Symbol
          return {
            kind: GridCellKind.Text,
            data: signal.symbol,
            displayData: signal.symbol,
            allowOverlay: false,
            themeOverride: { textDark: '#d2a8ff' },
          };

        case 3: // Strength
          return {
            kind: GridCellKind.Text,
            data: String(signal.strength),
            displayData: Number(signal.strength).toFixed(4),
            allowOverlay: false,
            themeOverride: { textDark: '#ffa657' },
          };

        case 4: // Strategy
          return {
            kind: GridCellKind.Text,
            data: signal.strategy_name,
            displayData: signal.strategy_name,
            allowOverlay: false,
          };

        case 5: // Metadata
          return {
            kind: GridCellKind.Text,
            data: signal.metadata ?? '',
            displayData: signal.metadata ?? '—',
            allowOverlay: false,
            themeOverride: { textDark: '#8b949e' },
          };

        default:
          return { kind: GridCellKind.Text, data: '', displayData: '', allowOverlay: false };
      }
    },
    [signals]
  );

  // Glide Data Grid needs columns to be stable refs
  const columns = useMemo(() => COLUMNS, []);

  if (signals.length === 0) {
    return (
      <div className="empty-state">
        <div className="icon">📡</div>
        <p>Waiting for signals…</p>
        <small>Signals will appear here as they stream in from the engine.</small>
      </div>
    );
  }

  return (
    <div className="grid-wrap">
      <DataEditor
        getCellContent={getCellContent}
        columns={columns}
        rows={signals.length}
        theme={DARK_THEME}
        width="100%"
        height="100%"
        rowHeight={34}
        headerHeight={36}
        smoothScrollX
        smoothScrollY
        rowMarkers="number"
        freezeColumns={0}
        verticalBorder
      />
    </div>
  );
}
