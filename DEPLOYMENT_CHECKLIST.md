# FP&A Copilot - llama3:latest Deployment Checklist

## Current Issue
**Error**: "model requires more system memory than is currently available unable to load full model on GPU"

## System Requirements for llama3:latest
- **RAM**: 16GB+ recommended (minimum 12GB)
- **Storage**: ~5GB free space for model
- **OS**: Windows 10/11 with updated pagefile settings

---

## Step-by-Step Resolution (Try in Order)

### 1. Check Ollama Installation

```bash
# Check Ollama version
ollama -v

# Check available models
ollama list

# Check if Ollama is running
curl http://localhost:11434/api/tags
```

**Expected**: Ollama version 0.1.x or higher, models list should show installed models

---

### 2. Increase Windows Pagefile (Virtual Memory)

This is often the PRIMARY solution for memory errors.

**Steps**:
1. Open `Settings` → `System` → `About`
2. Click `Advanced system settings`
3. Under `Performance`, click `Settings`
4. Go to `Advanced` tab → `Virtual memory` → Click `Change`
5. Uncheck "Automatically manage paging file size"
6. Select your system drive (usually C:)
7. Choose `Custom size`:
   - **Initial size**: 16384 MB (16 GB)
   - **Maximum size**: 32768 MB (32 GB)
8. Click `Set` → `OK` → Restart Windows

**After restart**, run the diagnostic endpoint:
```bash
curl http://localhost:8000/debug/ollama-model-status
```

---

### 3. Try Quantized llama3 Models

Quantized models use less memory while maintaining quality:

```bash
# Try 4-bit quantized variant (recommended)
ollama pull llama3:8b-q4_0

# OR try 4-bit k-quant medium
ollama pull llama3:8b-q4_k_m

# Test the model
ollama run llama3:8b-q4_0 "Hello, test message"
```

**If successful**, update `backend/.env`:
```
OLLAMA_MODEL=llama3:8b-q4_0
```

---

### 4. Try Smaller llama3 Variants

```bash
# llama3.2 3B model (much smaller, still good quality)
ollama pull llama3.2:3b

# llama3.2 1B model (smallest)
ollama pull llama3.2:1b

# Test
ollama run llama3.2:3b "Hello, test message"
```

---

### 5. Force CPU-Only Mode (If You Have Integrated GPU Conflicts)

Create/edit `backend/.env`:
```
OLLAMA_NUM_GPU=0
```

**OR** start Ollama with environment variable:
```bash
# PowerShell
$env:OLLAMA_NUM_GPU="0"
ollama serve

# CMD
set OLLAMA_NUM_GPU=0
ollama serve
```

---

### 6. Check System Resources

```bash
# Check memory usage (PowerShell)
Get-Counter '\Memory\Available MBytes'

# Check if other programs are using memory
# Close unnecessary applications, especially:
# - Chrome/Edge (can use 4-8GB)
# - Docker Desktop
# - Visual Studio / IDEs
```

---

### 7. Advanced: Manual Model Quantization

If you have the original GGUF file:

```bash
# Download llama.cpp
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp
mkdir build && cd build
cmake ..
cmake --build . --config Release

# Quantize model to 4-bit
cd ..
./build/bin/quantize <path-to-llama3-model> llama3-q4_0.gguf q4_0

# Add to Ollama
# Create Modelfile:
FROM ./llama3-q4_0.gguf

# Then:
ollama create llama3-custom -f Modelfile
ollama run llama3-custom "test"
```

---

## Verification Commands

After each attempt, run diagnostics:

```bash
# Backend diagnostic endpoint
curl http://localhost:8000/debug/ollama-model-status

# Test chat directly
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": false
  }'
```

---

## Frontend UI Flow

The app will automatically:
1. Call `/debug/ollama-model-status` on startup
2. Show progress for each attempt
3. Display results:
   - ✅ **Success**: "llama3:latest is ready"
   - ⚠️ **Fallback**: "Using llama3:8b-q4_0 (quantized) - llama3:latest requires more memory"
   - ❌ **Failed**: Shows detailed steps and recommendations

---

## Troubleshooting

### If All Steps Fail

**Option A**: Run on a different machine
- Minimum 16GB RAM recommended
- Can run Ollama on a server and point `OLLAMA_URL` to it

**Option B**: Use cloud-hosted Ollama
- Deploy Ollama on cloud VM (AWS, Azure, GCP)
- Update `backend/.env`: `OLLAMA_URL=http://<your-cloud-ip>:11434`

**Option C**: Use mistral:latest as permanent fallback
```bash
ollama pull mistral:latest
# Update backend/.env:
OLLAMA_MODEL=mistral:latest
```

---

## Expected Outcomes

| Scenario | Solution | Quality |
|----------|----------|---------|
| 16GB+ RAM, no GPU | llama3:latest works | ⭐⭐⭐⭐⭐ Best |
| 12-16GB RAM | llama3:8b-q4_0 works | ⭐⭐⭐⭐ Excellent |
| 8-12GB RAM | llama3.2:3b works | ⭐⭐⭐ Good |
| <8GB RAM | mistral:latest fallback | ⭐⭐ Acceptable |

---

## Log Files

Check logs for detailed diagnostics:
- `backend/logs/model-load.log` - Model loading attempts
- Console output from `uvicorn` - Runtime errors
- Ollama logs: `~/.ollama/logs/` or check Task Manager → Details → ollama.exe

---

## Support

If issues persist after all steps:
1. Check backend logs: `backend/logs/model-load.log`
2. Run diagnostic: `GET http://localhost:8000/debug/ollama-model-status`
3. Share the diagnostic output for further troubleshooting
