import { FileText, CheckCircle, AlertCircle } from 'lucide-react';
import { Badge } from './ui/badge';
import { Card } from './ui/card';

export interface UsedChunk {
  file_id?: string;
  filename: string;
  sheet: string;
  rows: string;
  score?: number;
  preview?: string;
}

interface ProvenanceDisplayProps {
  chunks: UsedChunk[];
  isDeterministic?: boolean;
}

export function ProvenanceDisplay({ chunks, isDeterministic }: ProvenanceDisplayProps) {
  if (!chunks || chunks.length === 0) return null;

  return (
    <Card className="mt-3 p-3 bg-muted/30 dark:bg-muted/10 border-primary/20">
      <div className="flex items-center gap-2 mb-2">
        {isDeterministic ? (
          <>
            <CheckCircle className="w-4 h-4 text-green-600 dark:text-green-400" />
            <span className="text-xs font-semibold text-foreground">✅ Deterministic (No LLM)</span>
          </>
        ) : (
          <>
            <FileText className="w-4 h-4 text-primary" />
            <span className="text-xs font-semibold text-foreground">Sources Used:</span>
          </>
        )}
      </div>

      <div className="space-y-1.5">
        {chunks.map((chunk, index) => (
          <div
            key={index}
            className="flex items-start gap-2 text-xs p-2 rounded bg-background/50 dark:bg-background/30 border border-border/50"
          >
            <FileText className="w-3 h-3 mt-0.5 text-muted-foreground flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <div className="font-medium text-foreground truncate" title={chunk.filename}>
                {chunk.filename}
              </div>
              <div className="flex flex-wrap gap-1.5 mt-1">
                {chunk.sheet && chunk.sheet !== 'N/A' && (
                  <Badge variant="secondary" className="text-xs px-1.5 py-0">Sheet: {chunk.sheet}</Badge>
                )}
                {chunk.rows && (
                  <Badge variant="secondary" className="text-xs px-1.5 py-0">Rows {chunk.rows}</Badge>
                )}
                {chunk.score !== undefined && (
                  <Badge variant="outline" className="text-xs px-1.5 py-0">Score: {chunk.score.toFixed(2)}</Badge>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>

      {!isDeterministic && (
        <div className="mt-2 flex items-start gap-1.5 text-xs text-muted-foreground">
          <AlertCircle className="w-3 h-3 mt-0.5 flex-shrink-0" />
          <span>Answer generated from retrieved data chunks. Verify important details.</span>
        </div>
      )}
    </Card>
  );
}
