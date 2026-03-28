// IndexedDB for local storage of chats and files
import { openDB, DBSchema, IDBPDatabase } from 'idb';

export interface Message {
  role: 'user' | 'assistant' | 'system';
  content: string;
  timestamp: number;
}

export interface Chat {
  id: string;
  title: string;
  messages: Message[];
  attachedFiles: string[]; // Array of file IDs attached to this chat
  createdAt: number;
  updatedAt: number;
}

export interface UploadedFile {
  id: string;
  name: string;
  type: string;
  size: number;
  data: ArrayBuffer;
  uploadedAt: number;
}

interface FPADatabase extends DBSchema {
  chats: {
    key: string;
    value: Chat;
    indexes: { 'by-date': number };
  };
  files: {
    key: string;
    value: UploadedFile;
    indexes: { 'by-date': number };
  };
}

let dbInstance: IDBPDatabase<FPADatabase> | null = null;

export async function getDB() {
  if (dbInstance) return dbInstance;

  dbInstance = await openDB<FPADatabase>('fpa-copilot', 1, {
    upgrade(db) {
      // Chats store
      if (!db.objectStoreNames.contains('chats')) {
        const chatStore = db.createObjectStore('chats', { keyPath: 'id' });
        chatStore.createIndex('by-date', 'updatedAt');
      }

      // Files store
      if (!db.objectStoreNames.contains('files')) {
        const fileStore = db.createObjectStore('files', { keyPath: 'id' });
        fileStore.createIndex('by-date', 'uploadedAt');
      }
    },
  });

  return dbInstance;
}

// Chat operations
export async function saveChat(chat: Chat) {
  const db = await getDB();
  await db.put('chats', chat);
}

export async function getChat(id: string): Promise<Chat | undefined> {
  const db = await getDB();
  return db.get('chats', id);
}

export async function getAllChats(): Promise<Chat[]> {
  const db = await getDB();
  const chats = await db.getAllFromIndex('chats', 'by-date');
  return chats.reverse(); // Most recent first
}

export async function deleteChat(id: string) {
  const db = await getDB();
  await db.delete('chats', id);
}

export async function clearAllChats() {
  const db = await getDB();
  await db.clear('chats');
}

// File operations
export async function saveFile(file: UploadedFile) {
  const db = await getDB();
  await db.put('files', file);
}

export async function getFile(id: string): Promise<UploadedFile | undefined> {
  const db = await getDB();
  return db.get('files', id);
}

export async function getAllFiles(): Promise<UploadedFile[]> {
  const db = await getDB();
  return db.getAllFromIndex('files', 'by-date');
}

export async function deleteFile(id: string) {
  const db = await getDB();
  await db.delete('files', id);
}

export async function clearAllFiles() {
  const db = await getDB();
  await db.clear('files');
}
