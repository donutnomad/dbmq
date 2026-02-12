import * as React from 'react';
import { cn } from '@/lib/utils';

type TableProps = React.HTMLAttributes<HTMLTableElement>;

type TableHeaderProps = React.HTMLAttributes<HTMLTableSectionElement>;

type TableBodyProps = React.HTMLAttributes<HTMLTableSectionElement>;

interface TableRowProps extends React.HTMLAttributes<HTMLTableRowElement> {
  expandable?: boolean;
}

interface TableHeadProps extends React.ThHTMLAttributes<HTMLTableCellElement> {
  sortable?: boolean;
}

type TableCellProps = React.TdHTMLAttributes<HTMLTableCellElement>;

export function Table({ className, children, ...props }: TableProps) {
  return (
    <div className="relative w-full overflow-auto">
      <table
        className={cn(
          "w-full caption-bottom text-sm border-collapse",
          className
        )}
        {...props}
      >
        {children}
      </table>
    </div>
  );
}

export function TableHeader({ className, ...props }: TableHeaderProps) {
  return (
    <thead
      className={cn(
        "sticky top-0 z-10 bg-gray-50 border-b border-gray-200",
        className
      )}
      {...props}
    />
  );
}

export function TableBody({ className, ...props }: TableBodyProps) {
  return (
    <tbody
      className={cn(
        "[&_tr:last-child]:border-0",
        className
      )}
      {...props}
    />
  );
}

export function TableRow({ className, expandable, ...props }: TableRowProps) {
  return (
    <tr
      className={cn(
        "border-b border-gray-200 transition-colors",
        "hover:bg-gray-50",
        "data-[state=selected]:bg-blue-50",
        expandable && "cursor-pointer",
        className
      )}
      {...props}
    />
  );
}

export function TableHead({ className, sortable, ...props }: TableHeadProps) {
  return (
    <th
      className={cn(
        "h-12 px-4 text-left align-middle font-medium text-gray-700",
        "whitespace-nowrap",
        sortable && "cursor-pointer select-none hover:bg-gray-100",
        className
      )}
      {...props}
    />
  );
}

export function TableCell({ className, ...props }: TableCellProps) {
  return (
    <td
      className={cn(
        "px-4 py-3 align-middle",
        className
      )}
      {...props}
    />
  );
}
