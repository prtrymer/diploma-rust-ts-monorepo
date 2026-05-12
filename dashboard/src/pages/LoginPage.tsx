import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useStore } from '../store/useStore.ts';
import type { AuthResponse, SymbolsResponse } from '../types';

type Mode = 'login' | 'register';

export default function LoginPage() {
  const [mode, setMode] = useState<Mode>('login');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const { setToken, setSymbols } = useStore();
  const navigate = useNavigate();

  // Clear stale error on input change or mode switch
  useEffect(() => { setError(''); }, [username, password, mode]);

  async function doLogin(user: string, pass: string): Promise<string> {
    const res = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: user, password: pass }),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body?.error ?? 'Invalid credentials.');
    }
    const data: AuthResponse = await res.json();
    return data.token;
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!username.trim() || !password) return;
    setLoading(true);
    setError('');

    try {
      if (mode === 'register') {
        // 1. Register
        const regRes = await fetch('/api/auth/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username: username.trim(), password }),
        });
        if (!regRes.ok) {
          const body = await regRes.json().catch(() => ({}));
          throw new Error(body?.error ?? 'Registration failed.');
        }
      }

      // 2. Login (always — after register OR directly)
      const token = await doLogin(username.trim(), password);

      // 3. Fetch portfolio symbols
      const symRes = await fetch('/api/symbols', {
        headers: { Authorization: `Bearer ${token}` },
      });
      const symData: SymbolsResponse = symRes.ok ? await symRes.json() : { symbols: [] };

      setToken(token, username.trim());
      setSymbols(symData.symbols);
      navigate('/dashboard', { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Network error — is the backend running?');
    } finally {
      setLoading(false);
    }
  }

  const isLogin = mode === 'login';

  return (
    <main className="login-root">
      <div className="login-card">
        <h1 className="login-logo">
          Trading<span>OS</span>
        </h1>
        <p className="login-subtitle">
          {isLogin ? 'Sign in to access your signal dashboard' : 'Create a new account'}
        </p>

        {error && <div className="login-error" role="alert">{error}</div>}

        <form onSubmit={handleSubmit} noValidate>
          <div className="form-group">
            <label htmlFor="username">Username</label>
            <input
              id="username"
              type="text"
              autoComplete={isLogin ? 'username' : 'new-username'}
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="your_username"
              required
              disabled={loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              autoComplete={isLogin ? 'current-password' : 'new-password'}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              required
              disabled={loading}
            />
          </div>
          <button
            id={isLogin ? 'btn-login' : 'btn-register'}
            type="submit"
            className="btn-primary"
            disabled={loading || !username.trim() || !password}
          >
            {loading
              ? isLogin ? 'Signing in…' : 'Creating account…'
              : isLogin ? 'Sign in' : 'Create account'}
          </button>
        </form>

        <p style={{ marginTop: '1.25rem', textAlign: 'center', fontSize: '0.82rem', color: 'var(--text-secondary)' }}>
          {isLogin ? "Don't have an account?" : 'Already have an account?'}{' '}
          <button
            id={isLogin ? 'btn-switch-register' : 'btn-switch-login'}
            onClick={() => setMode(isLogin ? 'register' : 'login')}
            style={{
              background: 'none', border: 'none', padding: 0,
              color: 'var(--accent-blue)', cursor: 'pointer',
              fontWeight: 600, fontSize: 'inherit',
            }}
          >
            {isLogin ? 'Register' : 'Sign in'}
          </button>
        </p>
      </div>
    </main>
  );
}

