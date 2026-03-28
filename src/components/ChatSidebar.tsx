import { Chat } from '@/lib/db';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Plus, MessageSquare, Trash2, FileText, X } from 'lucide-react';
import { cn } from '@/lib/utils';

interface ChatSidebarProps {
  chats: Chat[];
  currentChatId: string | null;
  onNewChat: () => void;
  onSelectChat: (chatId: string) => void;
  onDeleteChat: (chatId: string) => void;
}

export function ChatSidebar({
  chats,
  currentChatId,
  onNewChat,
  onSelectChat,
  onDeleteChat,
}: ChatSidebarProps) {
  return (
    <div className="w-64 bg-sidebar-bg text-sidebar-fg flex flex-col h-screen border-r border-sidebar-hover">
      {/* Header */}
      <div className="p-4 border-b border-sidebar-hover">
        <h1 className="text-xl font-bold mb-1">FP&A Copilot</h1>
        <p className="text-xs text-sidebar-fg/60">100% Local & Private</p>
      </div>

      {/* New Chat Button */}
      <div className="p-3 border-b border-sidebar-hover">
        <Button
          onClick={onNewChat}
          className="w-full bg-sidebar-active hover:bg-sidebar-active/90 text-white"
        >
          <Plus className="w-4 h-4 mr-2" />
          New Chat
        </Button>
      </div>

      {/* Chat History */}
      <ScrollArea className="flex-1">
        <div className="p-2">
          <div className="text-xs font-semibold text-sidebar-fg/60 px-2 mb-2">
            CONVERSATIONS
          </div>
          {chats.length === 0 ? (
            <div className="text-sm text-sidebar-fg/40 px-2 py-4 text-center">
              No chats yet
            </div>
          ) : (
            <div className="space-y-1">
              {chats.map((chat) => (
                <div
                  key={chat.id}
                  className={cn(
                    'group flex items-center gap-2 p-2 rounded-lg cursor-pointer transition-colors',
                    currentChatId === chat.id
                      ? 'bg-sidebar-active text-white'
                      : 'hover:bg-sidebar-hover'
                  )}
                  onClick={() => onSelectChat(chat.id)}
                >
                  <MessageSquare className="w-4 h-4 flex-shrink-0" />
                  <span className="flex-1 text-sm truncate">{chat.title}</span>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="opacity-0 group-hover:opacity-100 h-6 w-6 p-0 hover:bg-destructive/20"
                    onClick={(e) => {
                      e.stopPropagation();
                      onDeleteChat(chat.id);
                    }}
                  >
                    <Trash2 className="w-3 h-3" />
                  </Button>
                </div>
              ))}
            </div>
          )}
        </div>
      </ScrollArea>

      {/* Footer */}
      <div className="p-3 border-t border-sidebar-hover">
        <div className="text-xs text-sidebar-fg/40 text-center">
          Powered by Ollama
        </div>
      </div>
    </div>
  );
}
