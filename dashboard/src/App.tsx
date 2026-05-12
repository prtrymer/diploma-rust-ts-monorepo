import { Routes, Route, Navigate } from 'react-router-dom';
import { useStore } from './store/useStore';
import LoginPage from './pages/LoginPage.tsx';
import DashboardPage from './pages/DashboardPage.tsx';

export default function App() {
  const token = useStore((s) => s.token);

  return (
    <Routes>
      <Route path="/login" element={token ? <Navigate to="/dashboard" replace /> : <LoginPage />} />
      <Route path="/dashboard" element={token ? <DashboardPage /> : <Navigate to="/login" replace />} />
      <Route path="*" element={<Navigate to={token ? '/dashboard' : '/login'} replace />} />
    </Routes>
  );
}
