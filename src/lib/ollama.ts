// API client - connects to local FastAPI backend
const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || 'http://localhost:8000';

export interface AskResponse {
  question: string;
  session_id?: string;
  intent?: Record<string, any>;
  answer?: string;
  error?: string;
}

// Send question to /ask — session_id tells backend to remember conversation context
export async function askQuestion(question: string, sessionId: string, model?: string): Promise<AskResponse> {
  try {
    const params = new URLSearchParams({ question, session_id: sessionId });
    if (model) params.append('model', model);

    const response = await fetch(`${BACKEND_URL}/ask?${params.toString()}`, {
      method: 'POST',
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Backend error: ${text}`);
    }

    return await response.json();
  } catch (error) {
    console.error('API error:', error);
    throw new Error(
      error instanceof Error
        ? error.message
        : 'Failed to connect to backend. Make sure the FastAPI server is running on port 8000.'
    );
  }
}

// Upload Excel file — backend converts to parquet + schema + metadata and hot-reloads
export async function uploadFile(file: File): Promise<{
  success: boolean;
  message?: string;
  rows?: number;
  columns?: number;
  error?: string;
}> {
  try {
    const formData = new FormData();
    formData.append('file', file);

    const response = await fetch(`${BACKEND_URL}/upload`, {
      method: 'POST',
      body: formData,
    });

    const data = await response.json();

    if (!response.ok) {
      return { success: false, error: data.detail || `Upload failed: ${response.status}` };
    }

    return { success: true, message: data.message, rows: data.rows, columns: data.columns };
  } catch (error) {
    return { success: false, error: error instanceof Error ? error.message : 'Upload failed' };
  }
}

// Clear backend conversation history for a session
export async function clearConversation(sessionId: string): Promise<void> {
  try {
    await fetch(`${BACKEND_URL}/conversation/clear?session_id=${sessionId}`, {
      method: 'POST',
    });
  } catch {
    // silent fail — not critical
  }
}

// Get company names AND worker names for autocomplete dropdown
// Backend now returns { customers: string[], workers: string[] }
export async function getCustomers(): Promise<{ customers: string[]; workers: string[] }> {
  try {
    const response = await fetch(`${BACKEND_URL}/customers`, {
      method: 'GET',
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) return { customers: [], workers: [] };
    const data = await response.json();
    return {
      customers: data.customers || [],
      workers:   data.workers   || [],
    };
  } catch {
    return { customers: [], workers: [] };
  }
}

// Check backend health
export async function checkBackendHealth(): Promise<boolean> {
  try {
    const response = await fetch(`${BACKEND_URL}/health`, {
      method: 'GET',
      signal: AbortSignal.timeout(5000),
    });
    const isHealthy = response.ok;
    console.log(`Backend health check: ${isHealthy ? '✓ Connected' : '✗ Failed'} (${BACKEND_URL})`);
    return isHealthy;
  } catch (error) {
    console.error('Backend health check failed:', error);
    return false;
  }
}