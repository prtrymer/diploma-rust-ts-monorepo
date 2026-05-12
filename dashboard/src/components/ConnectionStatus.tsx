import { useStore } from '../store/useStore.ts';
import type { WsStatus } from '../types';

const LABEL: Record<WsStatus, string> = {
  connected: 'Connected',
  disconnected: 'Disconnected',
  connecting: 'Connecting…',
  error: 'Error',
};

export default function ConnectionStatus() {
  const status = useStore((s) => s.wsStatus);
  return (
    <span className={`ws-status ${status}`} title={`WebSocket: ${status}`}>
      <span className="dot" />
      {LABEL[status]}
    </span>
  );
}
