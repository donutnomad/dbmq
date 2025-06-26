import { cn } from '@/lib/utils';
import { LucideIcon } from 'lucide-react';

interface CardProps {
  className?: string;
  children: React.ReactNode;
}

interface StatCardProps {
  title: string;
  value: string | number | React.ReactNode;
  icon?: React.ReactNode;
  className?: string;
}

export function Card({ className, children }: CardProps) {
  return (
    <div className={cn("bg-white rounded-lg", className)}>
      {children}
    </div>
  )
}

export function StatCard({ title, value, icon, className }: StatCardProps) {
  return (
    <Card className={cn("p-4", className)}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          <p className="mt-1 text-xl font-semibold">{value}</p>
        </div>
        {icon && <div className="text-gray-400">{icon}</div>}
      </div>
    </Card>
  )
}

export function CardHeader({ className, children }: CardProps) {
  return (
    <div className={cn("p-4", className)}>
      {children}
    </div>
  )
}

export function CardTitle({ className, children }: CardProps) {
  return (
    <h3 className={cn("text-lg font-semibold", className)}>
      {children}
    </h3>
  )
}

export function CardContent({ className, children }: CardProps) {
  return (
    <div className={cn("p-4 pt-0", className)}>
      {children}
    </div>
  )
}

const CardFooter = ({ className, children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
  <div className={cn('flex items-center p-4 pt-0', className)} {...props}>
    {children}
  </div>
);

const CardDescription = ({ className, children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
  <p className={cn('text-sm text-muted-foreground', className)} {...props}>
    {children}
  </p>
);

export { CardFooter, CardDescription }; 