import { useEffect, useRef, useState } from 'react';
import { createChart, ColorType, CandlestickSeries } from 'lightweight-charts';
import type { IChartApi, ISeriesApi, Time } from 'lightweight-charts';
import { useStore } from '../store/useStore';

interface Props {
  symbol: string;
  token: string;
}

export default function ChartWidget({ symbol, token }: Props) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const { signals } = useStore();
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#8b949e',
      },
      grid: {
        vertLines: { color: 'rgba(48, 54, 61, 0.4)' },
        horzLines: { color: 'rgba(48, 54, 61, 0.4)' },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
      autoSize: true,
    });
    chartRef.current = chart;

    const series = chart.addSeries(CandlestickSeries, {
      upColor: '#3fb950',
      downColor: '#f85149',
      borderVisible: false,
      wickUpColor: '#3fb950',
      wickDownColor: '#f85149',
    });
    seriesRef.current = series;

    async function loadData() {
      setLoading(true);
      try {
        const res = await fetch(`/api/symbols/chart?symbol=${symbol}&timeframe=1m`, {
          headers: { Authorization: `Bearer ${token}` }
        });
        if (res.ok) {
          const data = await res.json();
          const sorted = data.candles.sort((a: any, b: any) => a.time - b.time);
          const formatted: any[] = [];
          let lastTime = 0;
          for (const c of sorted) {
            if (c.time > lastTime) {
              formatted.push({
                time: c.time as Time,
                open: c.open,
                high: c.high,
                low: c.low,
                close: c.close,
              });
              lastTime = c.time;
            }
          }
          try {
            series.setData(formatted);
          } catch (err) {
            console.error('Lightweight charts setData error:', err);
          }
        }
      } catch (e) {
        console.error('Failed to load chart data', e);
      } finally {
        setLoading(false);
      }
    }

    loadData();

    return () => {
      chart.remove();
    };
  }, [symbol, token]);

  // Update markers when signals change
  useEffect(() => {
    if (!seriesRef.current || signals.length === 0) return;
    
    // Filter signals for this symbol and format as markers
    const symbolSignals = signals.filter(s => s.symbol === symbol);
    // Sort by time
    // Convert to lightweight-charts markers
    const markers = symbolSignals.map(s => {
      // s.timestamp is ISO string
      const time = Math.floor(new Date(s.timestamp).getTime() / 1000) as Time;
      let color = '#58a6ff';
      let position: 'aboveBar' | 'belowBar' | 'inBar' = 'aboveBar';
      let shape: 'arrowUp' | 'arrowDown' | 'circle' = 'circle';
      let text = s.direction;
      
      if (s.direction === 'Long') {
        color = '#3fb950';
        position = 'belowBar';
        shape = 'arrowUp';
      } else if (s.direction === 'Short') {
        color = '#f85149';
        position = 'aboveBar';
        shape = 'arrowDown';
      }

      return {
        time,
        position,
        color,
        shape,
        text,
        size: 1,
      };
    }).sort((a: any, b: any) => a.time - b.time);
    
    try {
      // @ts-ignore - markers type issue
      seriesRef.current.setMarkers(markers);
    } catch (err) {
      console.error('Lightweight charts setMarkers error:', err);
    }
  }, [signals, symbol]);

  return (
    <div style={{ width: '100%', height: '300px', position: 'relative', borderBottom: '1px solid var(--bg-border)' }}>
      {loading && (
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 10 }}>
          Loading chart...
        </div>
      )}
      <div ref={chartContainerRef} style={{ width: '100%', height: '100%' }} />
    </div>
  );
}
