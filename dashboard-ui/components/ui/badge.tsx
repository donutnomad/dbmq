import { cn, getStatusColor, getStatusText } from '@/lib/utils';

interface BadgeProps {
  status?: string;
  className?: string;
  children?: React.ReactNode;
}

export function StatusBadge({ status, className, children }: BadgeProps) {
  const statusColor = getStatusColor(status);
  const statusText = children || getStatusText(status);

  return (
    <span
      className={cn(
        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
        statusColor,
        className
      )}
    >
      {statusText}
    </span>
  );
}

export function Badge({ children, className, variant = 'default' }: {
  children: React.ReactNode;
  className?: string;
  variant?: 'default' | 'success' | 'warning' | 'error' | 'info' | 'outline';
}) {
  const variantStyles = {
    default: 'bg-gray-100 text-gray-800',
    success: 'bg-green-100 text-green-800',
    warning: 'bg-yellow-100 text-yellow-800',
    error: 'bg-red-100 text-red-800',
    info: 'bg-blue-100 text-blue-800',
    outline: 'bg-white text-gray-800 border border-gray-200',
  };

  return (
    <span
      className={cn(
        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
        variantStyles[variant],
        className
      )}
    >
      {children}
    </span>
  );
} 