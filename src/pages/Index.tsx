import { useState, useEffect, useRef } from 'react';
import { ChatSidebar } from '@/components/ChatSidebar';
import { ChatMessage } from '@/components/ChatMessage';
import { ChatInput } from '@/components/ChatInput';
import { TypingIndicator } from '@/components/TypingIndicator';
import { ThemeToggle } from '@/components/ThemeToggle';
import { DataVisualization } from '@/components/DataVisualization';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useToast } from '@/hooks/use-toast';
import {
  Chat,
  Message,
  getAllChats,
  getChat,
  saveChat,
  deleteChat,
} from '@/lib/db';
import { askQuestion, checkBackendHealth, uploadFile, clearConversation } from '@/lib/ollama';
import { AlertCircle } from 'lucide-react';
import { Alert, AlertDescription } from '@/components/ui/alert';

// Safely converts any backend answer to a displayable string — prevents blank screen crashes
function formatAnswer(answer: unknown): string {
  if (answer === null || answer === undefined) return 'No answer returned.';
  if (typeof answer === 'string') return answer;
  if (typeof answer === 'number' || typeof answer === 'boolean') return String(answer);
  if (typeof answer === 'object') {
    const a = answer as Record<string, unknown>;
    // Unwrap nested answer field
    if (a.answer) return formatAnswer(a.answer);
    // Tabular result — format as markdown table
    if (a.columns && a.data) {
      const cols = a.columns as string[];
      const rows = a.data as unknown[][];
      const header = `| ${cols.join(' | ')} |`;
      const sep    = `| ${cols.map(() => '---').join(' | ')} |`;
      const body   = rows.map((r) => `| ${r.join(' | ')} |`).join('\n');
      return `${header}\n${sep}\n${body}`;
    }
    return JSON.stringify(answer, null, 2);
  }
  return String(answer);
}

const Index = () => {
  const [chats, setChats] = useState<Chat[]>([]);
  const [currentChatId, setCurrentChatId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [backendHealthy, setBackendHealthy] = useState<boolean>(true);
  const [showNewMessages, setShowNewMessages] = useState(false);
  const [userScrolledUp, setUserScrolledUp] = useState(false);

  // Generated once on load — passed with every /ask call so backend remembers context
  const sessionIdRef = useRef<string>(crypto.randomUUID());

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();

  // Backend health check
  useEffect(() => {
    let previousHealth = true;

    const checkHealth = async () => {
      const isHealthy = await checkBackendHealth();
      setBackendHealthy(isHealthy);

      if (!isHealthy && previousHealth) {
        toast({
          title: 'Backend unavailable',
          description: 'Backend not reachable. Start the FastAPI server on port 8000.',
          variant: 'destructive',
        });
      } else if (isHealthy && !previousHealth) {
        toast({
          title: 'Backend connected',
          description: 'Backend is now available.',
        });
      }

      previousHealth = isHealthy;
    };

    checkHealth();
    const interval = setInterval(checkHealth, 15000);
    return () => clearInterval(interval);
  }, []);

  // Load chats on mount
  useEffect(() => {
    loadChats();
  }, []);

  // Auto-scroll
  useEffect(() => {
    if (!scrollContainerRef.current || !messagesEndRef.current) return;

    const container = scrollContainerRef.current;
    const distanceFromBottom =
      container.scrollHeight - container.scrollTop - container.clientHeight;

    if (!userScrolledUp || distanceFromBottom < 100) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth', block: 'end' });
      setUserScrolledUp(false);
      setShowNewMessages(false);
    } else {
      setShowNewMessages(true);
    }
  }, [messages, isLoading]);

  const handleScroll = () => {
    if (!scrollContainerRef.current) return;
    const container = scrollContainerRef.current;
    const distanceFromBottom =
      container.scrollHeight - container.scrollTop - container.clientHeight;
    if (distanceFromBottom > 100) {
      setUserScrolledUp(true);
    } else {
      setUserScrolledUp(false);
      setShowNewMessages(false);
    }
  };

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    setUserScrolledUp(false);
    setShowNewMessages(false);
  };

  const loadChats = async () => {
    const loadedChats = await getAllChats();
    setChats(loadedChats);
  };

  // New chat — clear backend session and generate fresh session_id
  const handleNewChat = () => {
    clearConversation(sessionIdRef.current);
    sessionIdRef.current = crypto.randomUUID();
    setCurrentChatId(null);
    setMessages([]);
  };

  const handleSelectChat = async (chatId: string) => {
    const chat = await getChat(chatId);
    if (chat) {
      setCurrentChatId(chat.id);
      setMessages(chat.messages);
    }
  };

  const handleDeleteChat = async (chatId: string) => {
    await deleteChat(chatId);
    if (currentChatId === chatId) handleNewChat();
    loadChats();
  };

  const generateChatTitle = (firstMessage: string): string => {
    const words = firstMessage.split(' ').slice(0, 6).join(' ');
    return words.length > 40 ? words.substring(0, 40) + '...' : words;
  };

  // Send message — always passes session_id so backend remembers context
  const handleSendMessage = async (content: string) => {
    if (!content.trim() || isLoading) return;

    const userMessage: Message = { role: 'user', content, timestamp: Date.now() };
    const newMessages = [...messages, userMessage];
    setMessages(newMessages);
    setIsLoading(true);

    try {
      const response = await askQuestion(content, sessionIdRef.current);

      let answerContent: string;
      if (response.error) {
        answerContent = `⚠️ ${response.error}`;
      } else {
        // formatAnswer safely handles string, number, object, null — no more blank screens
        answerContent = formatAnswer(response.answer);
      }

      const assistantMessage: Message = {
        role: 'assistant',
        content: answerContent,
        timestamp: Date.now(),
      };

      const finalMessages = [...newMessages, assistantMessage];
      setMessages(finalMessages);

      let chatId = currentChatId;
      if (!chatId) {
        chatId = crypto.randomUUID();
        setCurrentChatId(chatId);
      }

      const chat: Chat = {
        id: chatId,
        title: currentChatId
          ? chats.find((c) => c.id === chatId)?.title || generateChatTitle(content)
          : generateChatTitle(content),
        messages: finalMessages,
        attachedFiles: [],
        createdAt: currentChatId
          ? chats.find((c) => c.id === chatId)?.createdAt || Date.now()
          : Date.now(),
        updatedAt: Date.now(),
      };

      await saveChat(chat);
      loadChats();
    } catch (error) {
      console.error('Chat error:', error);
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to get response.',
        variant: 'destructive',
      });
    } finally {
      setIsLoading(false);
    }
  };

  // File upload — calls POST /upload, resets session after new data loaded
  const handleFileUpload = async (file: File) => {
    const ext = file.name.split('.').pop()?.toLowerCase();
    if (ext !== 'xlsx' && ext !== 'xls') {
      toast({
        title: 'Invalid file type',
        description: 'Only .xlsx and .xls files are supported.',
        variant: 'destructive',
      });
      return;
    }

    toast({
      title: 'Uploading...',
      description: `Processing ${file.name}. This may take a few seconds.`,
    });

    const result = await uploadFile(file);

    if (result.success) {
      toast({
        title: 'File loaded ✓',
        description: `${file.name} — ${result.rows?.toLocaleString()} rows loaded. Bot is ready.`,
      });
      clearConversation(sessionIdRef.current);
      sessionIdRef.current = crypto.randomUUID();
    } else {
      toast({
        title: 'Upload failed',
        description: result.error || 'Something went wrong.',
        variant: 'destructive',
      });
    }
  };

  return (
    <div className="flex h-screen overflow-hidden bg-background text-foreground">
      <ChatSidebar
        chats={chats}
        currentChatId={currentChatId}
        onNewChat={handleNewChat}
        onSelectChat={handleSelectChat}
        onDeleteChat={handleDeleteChat}
      />

      <div className="flex-1 flex flex-col min-w-0">
        {/* Header */}
        <div className="border-b border-border bg-card dark:bg-card/50 px-4 py-3 flex items-center justify-between shadow-sm z-10">
          <h1 className="text-lg font-semibold text-foreground">FP&A Copilot</h1>
          <ThemeToggle />
        </div>

        {/* Backend status warning */}
        {backendHealthy === false && (
          <Alert
            variant="destructive"
            className="rounded-none border-x-0 border-t-0 bg-destructive/10 dark:bg-destructive/20"
          >
            <AlertCircle className="h-4 w-4" />
            <AlertDescription className="text-sm">
              Backend not reachable. Make sure FastAPI is running on port 8000.
            </AlertDescription>
          </Alert>
        )}

        {/* Main content */}
        <Tabs defaultValue="chat" className="flex-1 flex flex-col min-h-0 overflow-hidden">
          <TabsList className="mx-4 mt-2 w-fit bg-muted dark:bg-muted/50">
            <TabsTrigger
              value="chat"
              className="data-[state=active]:bg-background dark:data-[state=active]:bg-background/80"
            >
              Chat
            </TabsTrigger>
            <TabsTrigger
              value="visualize"
              className="data-[state=active]:bg-background dark:data-[state=active]:bg-background/80"
            >
              Visualize Data
            </TabsTrigger>
          </TabsList>

          <TabsContent
            value="chat"
            className="flex-1 flex flex-col mt-0 min-h-0 overflow-hidden"
          >
            <div
              ref={scrollContainerRef}
              onScroll={handleScroll}
              className="flex-1 overflow-y-auto scroll-smooth"
              style={{ overflowY: 'auto', height: '100%' }}
            >
              {messages.length === 0 ? (
                <div className="flex items-center justify-center h-full min-h-[calc(100vh-300px)] p-4">
                  <div className="text-center max-w-2xl">
                    <h1 className="text-4xl font-bold mb-4 text-foreground">
                      Welcome to FP&A Copilot
                    </h1>
                    <p className="text-lg text-muted-foreground mb-8">
                      Your local, private AI assistant for Financial Planning & Analysis
                    </p>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-left">
                      <div className="p-4 rounded-lg border border-border bg-card dark:bg-card/50">
                        <h3 className="font-semibold mb-2 text-card-foreground">
                          📊 Ask FP&A Questions
                        </h3>
                        <p className="text-sm text-muted-foreground">
                          Query your finance data — revenue, GM%, margins, trends, and more.
                        </p>
                      </div>
                      <div className="p-4 rounded-lg border border-border bg-card dark:bg-card/50">
                        <h3 className="font-semibold mb-2 text-card-foreground">🔒 100% Private</h3>
                        <p className="text-sm text-muted-foreground">
                          All processing happens locally via Ollama. Your data never leaves your
                          machine.
                        </p>
                      </div>
                      <div className="p-4 rounded-lg border border-border bg-card dark:bg-card/50">
                        <h3 className="font-semibold mb-2 text-card-foreground">
                          🧮 Deterministic Calculations
                        </h3>
                        <p className="text-sm text-muted-foreground">
                          All numbers computed by Pandas — the LLM only explains, never calculates.
                        </p>
                      </div>
                      <div className="p-4 rounded-lg border border-border bg-card dark:bg-card/50">
                        <h3 className="font-semibold mb-2 text-card-foreground">💡 Smart Analysis</h3>
                        <p className="text-sm text-muted-foreground">
                          Rankings, comparisons, trends, and grouped aggregations out of the box.
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="max-w-4xl mx-auto w-full px-4 py-6">
                  {messages.map((message, index) => (
                    <ChatMessage key={index} message={message} />
                  ))}
                  {isLoading && <TypingIndicator />}
                  <div ref={messagesEndRef} />
                </div>
              )}
            </div>

            {showNewMessages && (
              <div className="absolute bottom-24 left-1/2 transform -translate-x-1/2 z-20">
                <button
                  onClick={scrollToBottom}
                  className="px-4 py-2 bg-primary text-primary-foreground rounded-full shadow-lg hover:bg-primary/90 transition-all flex items-center gap-2 text-sm font-medium"
                >
                  <span>↓</span> New messages
                </button>
              </div>
            )}
          </TabsContent>

          <TabsContent value="visualize" className="flex-1 overflow-auto mt-0 min-h-0">
            <div className="max-w-4xl mx-auto p-4">
              <DataVisualization uploadedFiles={[]} />
            </div>
          </TabsContent>
        </Tabs>

        {/* Input */}
        <ChatInput
          onSendMessage={handleSendMessage}
          onFileUpload={handleFileUpload}
          isLoading={isLoading}
          disabled={false}
        />
      </div>
    </div>
  );
};

export default Index;