import { useState, useRef, useEffect, KeyboardEvent } from 'react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Send, Paperclip, Loader2 } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { getCustomers } from '@/lib/ollama';

interface ChatInputProps {
  onSendMessage: (message: string) => void;
  onFileUpload: (file: File) => Promise<void>;
  isLoading: boolean;
  disabled?: boolean;
}

interface SuggestionItem {
  label: string;
  type: 'company' | 'worker';
}

export function ChatInput({ onSendMessage, onFileUpload, isLoading, disabled }: ChatInputProps) {
  const [input, setInput] = useState('');
  const [uploading, setUploading] = useState(false);

  const [customers, setCustomers] = useState<string[]>([]);
  const [workers, setWorkers]     = useState<string[]>([]);
  const [suggestions, setSuggestions] = useState<SuggestionItem[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef  = useRef<HTMLTextAreaElement>(null);
  const dropdownRef  = useRef<HTMLDivElement>(null);
  const { toast } = useToast();

  useEffect(() => {
    getCustomers().then(({ customers: c, workers: w }) => {
      setCustomers(c);
      setWorkers(w);
    });
  }, []);

  const handleInputChange = (value: string) => {
    setInput(value);

    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 200) + 'px';
    }

    const trimmed = value.trim();
    if (trimmed.length < 2) {
      setShowSuggestions(false);
      return;
    }

    // ── CHANGE 1: Search only the last 4 words typed ──────────────────
    // So "tell me about abb" searches "abb", not the whole sentence
    const words = trimmed.split(/\s+/);
    const searchTerms: string[] = [];
    for (let i = words.length - 1; i >= Math.max(0, words.length - 4); i--) {
      searchTerms.push(words.slice(i).join(' ').toLowerCase());
    }

    const matchedCompanies: SuggestionItem[] = customers
      .filter(c => searchTerms.some(s => s.length >= 2 && c.toLowerCase().includes(s)))
      .slice(0, 8)  // ── CHANGE 2: show up to 8 per section (was 5)
      .map(c => ({ label: c, type: 'company' }));

    const matchedWorkers: SuggestionItem[] = workers
      .filter(w => searchTerms.some(s => s.length >= 2 && w.toLowerCase().includes(s)))
      .slice(0, 8)  // ── CHANGE 2: show up to 8 per section (was 5)
      .map(w => ({ label: w, type: 'worker' }));

    const combined = [...matchedCompanies, ...matchedWorkers];

    // ── CHANGE 3: removed the `combined.length <= 12` cap that was hiding results
    if (combined.length > 0) {
      setSuggestions(combined);
      setShowSuggestions(true);
      setHighlightedIndex(-1);
    } else {
      setShowSuggestions(false);
    }
  };

  const handleSelectSuggestion = (item: SuggestionItem) => {
    const words = input.split(' ');
    let replaced = false;
    for (let i = words.length - 1; i >= 0; i--) {
      const partial = words.slice(i).join(' ').trim();
      if (partial.length >= 2 && item.label.toLowerCase().includes(partial.toLowerCase())) {
        const before = words.slice(0, i).join(' ');
        setInput(before ? `${before} ${item.label}` : item.label);
        replaced = true;
        break;
      }
    }
    if (!replaced) setInput(item.label);

    setShowSuggestions(false);
    setHighlightedIndex(-1);
    textareaRef.current?.focus();
  };

  const handleSend = () => {
    if (!input.trim() || isLoading) return;
    setShowSuggestions(false);
    onSendMessage(input.trim());
    setInput('');
    if (textareaRef.current) textareaRef.current.style.height = 'auto';
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (showSuggestions) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setHighlightedIndex(i => Math.min(i + 1, suggestions.length - 1));
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setHighlightedIndex(i => Math.max(i - 1, -1));
        return;
      }
      if (e.key === 'Tab' || (e.key === 'Enter' && highlightedIndex >= 0)) {
        e.preventDefault();
        const target = highlightedIndex >= 0 ? suggestions[highlightedIndex] : suggestions[0];
        if (target) handleSelectSuggestion(target);
        return;
      }
      if (e.key === 'Escape') {
        setShowSuggestions(false);
        return;
      }
    }
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node))
        setShowSuggestions(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const validTypes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/pdf',
    ];

    if (!validTypes.includes(file.type)) {
      toast({ title: 'Invalid file type', description: 'Please upload CSV, Excel, or PDF files only.', variant: 'destructive' });
      return;
    }
    if (file.size > 50 * 1024 * 1024) {
      toast({ title: 'File too large', description: 'Maximum file size is 50MB.', variant: 'destructive' });
      return;
    }

    setUploading(true);
    try {
      await onFileUpload(file);
      toast({ title: 'File uploaded', description: `${file.name} has been uploaded successfully.` });
      getCustomers().then(({ customers: c, workers: w }) => { setCustomers(c); setWorkers(w); });
    } catch (error) {
      toast({ title: 'Upload failed', description: error instanceof Error ? error.message : 'Failed to upload file.', variant: 'destructive' });
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const companyItems = suggestions.filter(s => s.type === 'company');
  const workerItems  = suggestions.filter(s => s.type === 'worker');

  return (
    <div className="border-t border-border bg-background p-4">
      <div className="max-w-4xl mx-auto">
        <div className="flex gap-2 items-end relative" ref={dropdownRef}>
          <input ref={fileInputRef} type="file" accept=".csv,.xlsx,.xls,.pdf" onChange={handleFileSelect} className="hidden" />

          <Button
            variant="outline" size="icon"
            onClick={() => fileInputRef.current?.click()}
            disabled={uploading || disabled}
            className="flex-shrink-0"
          >
            {uploading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Paperclip className="w-5 h-5" />}
          </Button>

          <div className="flex-1 relative">
            <Textarea
              ref={textareaRef}
              value={input}
              onChange={e => handleInputChange(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask about FP&A or type a company / worker name..."
              disabled={disabled || isLoading}
              className="resize-none min-h-[44px] max-h-[200px] pr-12"
              rows={1}
            />

            {showSuggestions && suggestions.length > 0 && (
              <div className="absolute bottom-full left-0 right-0 mb-1 bg-popover border border-border rounded-lg shadow-lg z-50 overflow-hidden max-h-72 overflow-y-auto">

                {companyItems.length > 0 && (
                  <>
                    <div className="px-3 py-1.5 text-xs font-semibold text-muted-foreground bg-muted/50 border-b border-border sticky top-0">
                      🏢 Companies
                    </div>
                    {companyItems.map((item, i) => (
                      <button
                        key={`c-${item.label}`}
                        onMouseDown={e => { e.preventDefault(); handleSelectSuggestion(item); }}
                        className={`w-full text-left px-3 py-2 text-sm hover:bg-accent hover:text-accent-foreground transition-colors ${
                          i === highlightedIndex ? 'bg-accent text-accent-foreground' : ''
                        }`}
                      >
                        {item.label}
                      </button>
                    ))}
                  </>
                )}

                {workerItems.length > 0 && (
                  <>
                    <div className="px-3 py-1.5 text-xs font-semibold text-muted-foreground bg-muted/50 border-b border-border sticky top-0">
                      👤 Workers
                    </div>
                    {workerItems.map((item, i) => (
                      <button
                        key={`w-${item.label}`}
                        onMouseDown={e => { e.preventDefault(); handleSelectSuggestion(item); }}
                        className={`w-full text-left px-3 py-2 text-sm hover:bg-accent hover:text-accent-foreground transition-colors ${
                          companyItems.length + i === highlightedIndex ? 'bg-accent text-accent-foreground' : ''
                        }`}
                      >
                        {item.label}
                      </button>
                    ))}
                  </>
                )}

                <div className="px-3 py-1 text-xs text-muted-foreground border-t border-border bg-muted/20">
                  ↑↓ navigate · Tab to select · Esc to close
                </div>
              </div>
            )}
          </div>

          <Button
            onClick={handleSend}
            disabled={!input.trim() || isLoading || disabled}
            className="flex-shrink-0 bg-primary hover:bg-primary-hover"
          >
            {isLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Send className="w-5 h-5" />}
          </Button>
        </div>

        <div className="text-xs text-muted-foreground mt-2 text-center">
          Press Enter to send · Shift+Enter for new line · Tab to autocomplete
        </div>
      </div>
    </div>
  );
}