# Buckets

`buckets` is a library built on top of [`lmdbx`](https://www.npmjs.com/package/lmdbx), providing a bucket-based abstraction for managing databases with global change tracking.

## ✨ Features

- Bucket-based database organization
- Global change notifications (`put` and `remove`) for all buckets
- Type-safe API for keys and values
- Efficient storage using lmdbx

## 📦 Installation

```bash
npm install patrykwegrzyn/buckets
```

## 🚀 Quick Start

```typescript
import { Store } from "buckets";

// Create a new Store
const store = new Store("all-data");

// Create a bucket (database namespace)

type User = {
  name: string;
  age: number;
}

const users = store.bucket<User>("users");

// Listen for changes
store.on("change", (change) => {
  console.log("Change detected:", change);
});

// Add a user
await users.put("user:1", { name: "Alice", age: 30 });

// Retrieve a user
const user = users.get("user:1");
console.log("User retrieved:", user);

// Remove a user
await users.remove("user:1");
```

## 📚 API Reference

### `Store`

#### `new Store(name: string, options: RootDatabaseOptions)`
Creates a new `Store` instance.

- `name` - Name of the LMDB environment.
- `options` - LMDB environment options.

#### `store.bucket<TV = any>(name: string, options?: DatabaseOptions)`
Creates or retrieves a bucket (database namespace).

- `name` - Name of the bucket.
- `options` - LMDB database options.

#### Events

- `change` - Emitted when `put` or `remove` operations occur.

```typescript
store.on("change", (event) => {
  console.log("Change event:", event);
});
```

Event object:
```typescript
{
  type: "put" | "remove";
  bucket: string;
  id: string;
  value: any;
  version?: number;
}
```

## 🛠️ Options

- `cache` - Enables caching for faster lookups.
- `path` - Path to store LMDB files.

## ⚖️ License

MIT License © 2024 buckets
