import { Bot } from 'lucide-react';

export function TypingIndicator() {
  return (
    <div className="flex gap-4 p-4 animate-fade-in">
      <div className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center bg-primary">
        <Bot className="w-5 h-5 text-primary-foreground" />
      </div>
      
      <div className="flex-1 min-w-0 pt-1">
        <div className="font-semibold text-sm mb-2 text-foreground">
          FP&A Copilot
        </div>
        <div className="flex gap-1">
          <div className="w-2 h-2 bg-primary rounded-full animate-pulse-soft" style={{ animationDelay: '0ms' }} />
          <div className="w-2 h-2 bg-primary rounded-full animate-pulse-soft" style={{ animationDelay: '200ms' }} />
          <div className="w-2 h-2 bg-primary rounded-full animate-pulse-soft" style={{ animationDelay: '400ms' }} />
        </div>
      </div>
    </div>
  );
}
