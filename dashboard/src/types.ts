// ─── Signal types (mirrors Rust SignalEvent) ──────────────────────────────────

export type SignalDirection = 'Long' | 'Short' | 'Exit';

export interface SignalEvent {
  id: string;
  timestamp: string; // ISO-8601
  symbol: string;
  direction: SignalDirection;
  strength: number;
  strategy_name: string;
  metadata?: string | null;
}

// ─── WebSocket status ─────────────────────────────────────────────────────────

export type WsStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

// ─── API response shapes ──────────────────────────────────────────────────────

export interface AuthResponse {
  token: string;
  message: string;
}

export interface SymbolsResponse {
  symbols: string[];
}

export interface MessageResponse {
  message: string;
}
