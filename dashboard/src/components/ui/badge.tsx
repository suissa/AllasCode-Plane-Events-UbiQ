import * as React from 'react';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '../../lib/utils';

const badgeVariants = cva('inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors', {
  variants: {
    variant: {
      default: 'border-transparent bg-cyan-500/20 text-cyan-200',
      success: 'border-emerald-400/30 bg-emerald-500/15 text-emerald-200',
      warning: 'border-amber-400/30 bg-amber-500/15 text-amber-200',
      destructive: 'border-rose-400/30 bg-rose-500/15 text-rose-200',
      secondary: 'border-white/10 bg-white/10 text-slate-200',
    },
  },
  defaultVariants: {
    variant: 'default',
  },
});

export interface BadgeProps extends React.HTMLAttributes<HTMLDivElement>, VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}
