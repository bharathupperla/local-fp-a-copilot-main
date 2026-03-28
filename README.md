# FP&A Copilot - 100% Local AI Assistant

A completely local Financial Planning & Analysis assistant powered by Ollama. All data processing, embeddings, and AI inference happen on your machine - no external API calls.

## Features

✅ **Semantic RAG with Embeddings** - Intelligent chunk retrieval using vector similarity  
✅ **Multi-file Support** - Upload CSV, Excel (XLSX/XLS), and PDF files  
✅ **Per-chat File Attachments** - Each conversation has its own context  
✅ **Smart Auto-scroll** - Scroll freely while reading; new messages don't interrupt  
✅ **Dark Mode** - Beautiful dark/light themes with system preference detection  
✅ **100% Private** - Everything runs locally via Ollama  
✅ **No 500 Errors** - Robust error handling with graceful fallbacks  

## Prerequisites

1. **Ollama** - Download from [ollama.ai](https://ollama.ai)
2. **Python 3.9+** - For FastAPI backend
3. **Node.js 18+** - For React frontend

## Setup

### 1. Install Ollama Models

```bash
# Install the main chat model
ollama pull llama3:latest

# Install the embedding model for semantic search
ollama pull nomic-embed-text
```

### 2. Backend Setup

```bash
cd backend
pip install -r requirements.txt

# Start the backend server
python main.py
# Backend runs at http://localhost:8000
```

### 3. Frontend Setup

```bash
# In the project root
npm install

# Start the development server
npm run dev
# Frontend runs at http://localhost:8080
```

## Usage

### Upload Files
1. Click the 📎 paperclip icon in the chat input
2. Select CSV, Excel, or PDF files
3. Files are chunked and embedded automatically
4. Status indicators show: Uploading → Parsing → Processing → ✓ Ready

### Ask Questions
- **With files attached**: Questions are answered using semantic search over your data
- **Without files**: General FP&A knowledge from the model

### Example Queries
- "List all stocks in SEP"
- "What is the total revenue for Q3?"
- "Show me the variance between budget and actual"
- "Which departments had the highest expenses?"

## Architecture

### Backend (FastAPI)
- **Semantic RAG**: Chunks data into 5-8 row segments
- **Embeddings**: Uses Ollama's `nomic-embed-text` model
- **Vector Search**: Cosine similarity for top-K retrieval
- **Fallback**: Keyword matching when embeddings unavailable
- **Error Handling**: Never returns 500 errors; always safe JSON responses

### Frontend (React + Vite)
- **Smart Scroll**: Auto-scroll only when near bottom
- **File Status**: Real-time upload/parsing/embedding progress
- **Dark Mode**: Global theme with localStorage persistence
- **Non-blocking Errors**: Backend issues show toasts, not blocking modals

## Configuration

Create `.env` files:

**Frontend `.env`:**
```env
VITE_BACKEND_URL=http://localhost:8000
```

**Backend `backend/.env`:**
```env
OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=llama3:latest
EMBEDDING_MODEL=nomic-embed-text
```

## Troubleshooting

### Backend not connecting
- Check if Ollama is running: `ollama list`
- Verify backend is running: `curl http://localhost:8000/health`
- Check console for CORS errors

### Slow responses
- Embeddings take time for large files (normal)
- First query per session may be slower (model loading)
- Consider using smaller chunk sizes for large datasets

### Missing data in answers
- Ensure file status shows "✓ Ready" before querying
- Check that files are attached to the current chat
- Verify the question contains relevant keywords from your data

## Test Cases

1. **Upload CSV with stocks/months** → Ask "List all stocks in SEP" → Should return complete list
2. **Two chats with different files** → Answers should be context-specific
3. **Scroll test** → Scroll up → New messages should NOT force scroll → "New messages" pill appears
4. **Ollama offline** → Shows non-blocking warning → Chat input remains usable
5. **API stability** → No 500 errors even when Ollama is down

## Tech Stack

- **Frontend**: React 18, TypeScript, Tailwind CSS, Vite
- **Backend**: FastAPI, Python, Pandas, NumPy
- **AI**: Ollama (llama3, nomic-embed-text)
- **Storage**: IndexedDB (browser), In-memory (backend)

## License

MIT License - Use freely for personal or commercial projects.

---

**Built with ❤️ for privacy-conscious FP&A professionals**
