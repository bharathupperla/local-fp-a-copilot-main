import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CheckCircle2, XCircle, Loader2, AlertTriangle } from 'lucide-react';
import { ScrollArea } from '@/components/ui/scroll-area';

interface Attempt {
  step: string;
  timestamp: string;
  status: 'pending' | 'success' | 'failed';
  result?: string;
  raw_response?: any;
}

interface ModelStatusResponse {
  success: boolean;
  loaded_model: string | null;
  message: string;
  recommendation?: string;
  attempts: Attempt[];
}

interface ModelLoadingStatusProps {
  backendUrl: string;
}

export function ModelLoadingStatus({ backendUrl }: ModelLoadingStatusProps) {
  const [open, setOpen] = useState(true);
  const [loading, setLoading] = useState(true);
  const [status, setStatus] = useState<ModelStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const checkModelStatus = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`${backendUrl}/debug/ollama-model-status`);
      const data = await response.json();
      setStatus(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to check model status');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    checkModelStatus();
  }, []);

  const getStatusIcon = (attemptStatus: string) => {
    switch (attemptStatus) {
      case 'success':
        return <CheckCircle2 className="h-4 w-4 text-green-500" />;
      case 'failed':
        return <XCircle className="h-4 w-4 text-red-500" />;
      default:
        return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />;
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="max-w-2xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle>Model Loading Status</DialogTitle>
          <DialogDescription>
            Checking if llama3:latest can be loaded on your system
          </DialogDescription>
        </DialogHeader>

        {loading && (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
            <span className="ml-3 text-muted-foreground">
              Testing model configurations...
            </span>
          </div>
        )}

        {error && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {status && (
          <div className="space-y-4">
            {/* Summary */}
            <Alert variant={status.success ? 'default' : 'destructive'}>
              {status.success ? (
                <CheckCircle2 className="h-4 w-4" />
              ) : (
                <AlertTriangle className="h-4 w-4" />
              )}
              <AlertDescription>
                <div className="font-semibold mb-1">{status.message}</div>
                {status.loaded_model && (
                  <div className="text-sm">
                    Active model: <code className="bg-muted px-1 py-0.5 rounded">{status.loaded_model}</code>
                  </div>
                )}
              </AlertDescription>
            </Alert>

            {/* Recommendation */}
            {status.recommendation && (
              <Alert>
                <AlertDescription>
                  <div className="font-semibold mb-2">Recommendations:</div>
                  <pre className="text-xs whitespace-pre-wrap bg-muted p-2 rounded">
                    {status.recommendation}
                  </pre>
                </AlertDescription>
              </Alert>
            )}

            {/* Attempts log */}
            <div className="border rounded-lg p-3 bg-card">
              <h4 className="font-semibold mb-2 text-sm">Diagnostic Steps:</h4>
              <ScrollArea className="h-[200px]">
                <div className="space-y-2">
                  {status.attempts.map((attempt, idx) => (
                    <div key={idx} className="flex gap-2 text-xs border-b pb-2 last:border-0">
                      <div className="mt-0.5">{getStatusIcon(attempt.status)}</div>
                      <div className="flex-1">
                        <div className="font-medium">{attempt.step}</div>
                        {attempt.result && (
                          <div className="text-muted-foreground mt-1">{attempt.result}</div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </ScrollArea>
            </div>

            {/* Actions */}
            <div className="flex gap-2 justify-end">
              <Button variant="outline" onClick={checkModelStatus}>
                Retry
              </Button>
              <Button onClick={() => setOpen(false)}>
                {status.success ? 'Continue' : 'Proceed Anyway'}
              </Button>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
