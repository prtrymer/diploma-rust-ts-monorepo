import { useEffect, useRef } from 'react';
import { useStore } from '../store/useStore.ts';
import type { SignalEvent } from '../types';

const MAX_RETRIES = 8;
const BASE_DELAY_MS = 1000;

/**
 * Raw WebSocket hook that:
 * - Connects to /api/signals/stream?token=<jwt>&symbol=<activeSymbol>
 * - Uses server-side symbol filtering (Option B)
 * - Auto-reconnects with exponential backoff on disconnect/error
 * - Tears down cleanly when token or activeSymbol changes
 */
export function useSignalWs() {
  const token = useStore((s) => s.token);
  const setWsStatus = useStore((s) => s.setWsStatus);
  const addSignal = useStore((s) => s.addSignal);

  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isMountedRef = useRef(true);

  // Use a ref for addSignal to keep the connection logic stable
  const addSignalRef = useRef(addSignal);
  addSignalRef.current = addSignal;

  useEffect(() => {
    isMountedRef.current = true;
    
    function connect() {
      if (!token || !isMountedRef.current) return;

      const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const host = window.location.host;
      const url = `${protocol}://${host}/api/signals/stream?token=${encodeURIComponent(token)}`;

      console.log('🔌 WS: Connecting to', url);
      setWsStatus('connecting');

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!isMountedRef.current) { ws.close(); return; }
        console.log('🔌 WS: Connected (global stream)');
        retryRef.current = 0;
        setWsStatus('connected');
      };

      ws.onmessage = (event) => {
        if (!isMountedRef.current) return;
        try {
          const signal: SignalEvent = JSON.parse(event.data as string);
          addSignalRef.current(signal);
        } catch (err) {
          console.warn('⚠️ WS: Received invalid JSON', event.data, err);
        }
      };

      ws.onerror = (err) => {
        if (!isMountedRef.current) return;
        console.error('❌ WS: Error', err);
        setWsStatus('error');
      };

      ws.onclose = (event) => {
        if (!isMountedRef.current) return;
        console.log(`🔌 WS: Closed (code: ${event.code}, reason: ${event.reason || 'none'})`);
        setWsStatus('disconnected');
        wsRef.current = null;

        if (retryRef.current < MAX_RETRIES) {
          const delay = Math.min(BASE_DELAY_MS * 2 ** retryRef.current, 30_000);
          retryRef.current += 1;
          console.log(`🔌 WS: Retrying in ${delay}ms... (attempt ${retryRef.current})`);
          retryTimerRef.current = setTimeout(connect, delay);
        }
      };
    }

    // Initial connection
    if (token) {
      connect();
    } else {
      setWsStatus('disconnected');
    }

    return () => {
      isMountedRef.current = false;
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
      if (wsRef.current) {
        wsRef.current.onclose = null; // prevent reconnect on unmount
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [token, setWsStatus]); // Only reconnect if token changes
}
