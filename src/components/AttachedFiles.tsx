import { X, FileText, FileSpreadsheet, FileIcon } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

interface AttachedFilesProps {
  files: Array<{ id: string; name: string; type?: string; status?: string }>;
  onRemove: (fileId: string) => void;
}

export const AttachedFiles = ({ files, onRemove }: AttachedFilesProps) => {
  if (files.length === 0) return null;

  const getFileIcon = (fileName: string, type?: string) => {
    const ext = fileName.split('.').pop()?.toLowerCase();
    if (ext === 'csv' || ext === 'xlsx' || ext === 'xls' || type?.includes('spreadsheet')) {
      return <FileSpreadsheet className="h-4 w-4" />;
    }
    if (ext === 'pdf') {
      return <FileText className="h-4 w-4" />;
    }
    return <FileIcon className="h-4 w-4" />;
  };

  const getStatusBadge = (status?: string) => {
    if (status === 'uploading') return <span className="text-xs text-muted-foreground">Uploading...</span>;
    if (status === 'parsing') return <span className="text-xs text-muted-foreground">Parsing...</span>;
    if (status === 'embedding') return <span className="text-xs text-muted-foreground">Processing...</span>;
    if (status === 'ready') return <span className="text-xs text-green-600 dark:text-green-400">✓ Ready</span>;
    if (status === 'error') return <span className="text-xs text-destructive">Error</span>;
    return null;
  };

  return (
    <div className="border-b border-border bg-card dark:bg-card/50 px-4 py-3 shadow-sm">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-xs text-muted-foreground font-semibold uppercase tracking-wide">Attached:</span>
        {files.map((file) => (
          <div
            key={file.id}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-primary/10 dark:bg-primary/20 border border-primary/20 dark:border-primary/30 hover:bg-primary/15 dark:hover:bg-primary/25 transition-all group"
          >
            <span className="text-primary dark:text-primary-foreground">
              {getFileIcon(file.name, file.type)}
            </span>
            <span className="text-xs font-medium text-foreground max-w-[150px] truncate" title={file.name}>
              {file.name}
            </span>
            {getStatusBadge(file.status)}
            <Button
              variant="ghost"
              size="sm"
              className="h-4 w-4 p-0 hover:bg-destructive/30 dark:hover:bg-destructive/40 rounded-full ml-0.5 opacity-70 group-hover:opacity-100 transition-opacity"
              onClick={() => onRemove(file.id)}
              title="Remove attachment"
            >
              <X className="h-3 w-3 text-destructive dark:text-destructive-foreground" />
            </Button>
          </div>
        ))}
      </div>
    </div>
  );
};
