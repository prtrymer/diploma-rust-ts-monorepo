import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { SignalEvent, WsStatus } from '../types';

const MAX_SIGNALS = 500; // keep at most 500 signals in memory

// ─── Store shape ──────────────────────────────────────────────────────────────

interface AppStore {
  // Auth
  token: string | null;
  username: string | null;

  // Portfolio
  activeSymbol: string | null;
  symbols: string[];

  // Live signals (filtered to activeSymbol)
  signals: SignalEvent[];

  // WS connection status
  wsStatus: WsStatus;

  // ─── Actions ─────────────────────────────────────────────────────────────
  setToken: (token: string, username: string) => void;
  logout: () => void;

  setActiveSymbol: (symbol: string) => void;
  setSymbols: (symbols: string[]) => void;
  addSymbolToPortfolio: (symbol: string) => void;
  removeSymbolFromPortfolio: (symbol: string) => void;

  addSignal: (signal: SignalEvent) => void;
  clearSignals: () => void;

  setWsStatus: (status: WsStatus) => void;
}

// ─── Store implementation ─────────────────────────────────────────────────────

export const useStore = create<AppStore>()(
  persist(
    (set) => ({
      token: null,
      username: null,
      activeSymbol: null,
      symbols: [],
      signals: [],
      wsStatus: 'disconnected',

      setToken: (token, username) =>
        set({ token, username }),

      logout: () =>
        set({
          token: null,
          username: null,
          activeSymbol: null,
          symbols: [],
          signals: [],
          wsStatus: 'disconnected',
        }),

      setActiveSymbol: (symbol) =>
        set({ activeSymbol: symbol }), // no longer clearing signals here

      setSymbols: (symbols) => set({ symbols }),

      addSymbolToPortfolio: (symbol) =>
        set((state) => ({
          symbols: state.symbols.includes(symbol.toUpperCase())
            ? state.symbols
            : [...state.symbols, symbol.toUpperCase()],
        })),

      removeSymbolFromPortfolio: (symbol) =>
        set((state) => ({
          symbols: state.symbols.filter((s) => s !== symbol.toUpperCase()),
          activeSymbol:
            state.activeSymbol === symbol.toUpperCase()
              ? null
              : state.activeSymbol,
          signals: state.signals.filter((s) => s.symbol.toUpperCase() !== symbol.toUpperCase()),
        })),

      addSignal: (signal) =>
        set((state) => ({
          signals: [signal, ...state.signals].slice(0, MAX_SIGNALS),
        })),

      clearSignals: () => set({ signals: [] }),

      setWsStatus: (wsStatus) => set({ wsStatus }),
    }),
    {
      name: 'trading-dashboard',
      // Only persist auth + portfolio — not live signal data
      partialize: (state) => ({
        token: state.token,
        username: state.username,
        activeSymbol: state.activeSymbol,
        symbols: state.symbols,
      }),
    }
  )
);
