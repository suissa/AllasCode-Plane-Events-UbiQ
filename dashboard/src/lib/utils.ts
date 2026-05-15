import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function queueGradient(queue: string) {
  const palette = [
    'from-cyan-500/25 via-sky-500/10 to-blue-700/25 border-cyan-400/30',
    'from-violet-500/25 via-fuchsia-500/10 to-purple-800/25 border-violet-400/30',
    'from-emerald-500/25 via-teal-500/10 to-green-800/25 border-emerald-400/30',
    'from-amber-500/25 via-orange-500/10 to-red-800/25 border-amber-400/30',
    'from-rose-500/25 via-pink-500/10 to-red-800/25 border-rose-400/30',
    'from-lime-500/25 via-yellow-500/10 to-green-800/25 border-lime-400/30',
  ];

  const hash = queue.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
  return palette[hash % palette.length];
}

export function formatLatency(ms: number) {
  return `${Math.max(1, Math.round(ms))}ms`;
}

export function relativeTime(timestamp: string) {
  const delta = Date.now() - new Date(timestamp).getTime();
  if (delta < 1_000) return 'agora';
  if (delta < 60_000) return `${Math.floor(delta / 1_000)}s atrás`;
  return `${Math.floor(delta / 60_000)}min atrás`;
}
