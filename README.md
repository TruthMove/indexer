# aptos-indexer

This module has been thoroughly tested by processing the complete transaction history of both Aptos Testnet and Mainnet using Bun and SQLite (via. `bun:sqlite`).

## Prerequisites

- [Bun](https://bun.sh) v1.2.2 or later
- An Aptos API key from [Aptos Labs](https://aptoslabs.com/developers)

## Installation

```bash
bun install https://github.com/lithdew/aptos-indexer
```

## Quick Start

```typescript
import { streamTransactions } from "aptos-indexer";

// Stream transactions from version 0
for await (const event of streamTransactions({
  url: "grpc.testnet.aptoslabs.com:443",
  apiKey: process.env.APTOS_API_KEY_TESTNET!,
  startingVersion: 0n,
})) {
  switch (event.type) {
    case "data": {
      console.debug(`Got ${event.transactions.length} transaction(s)`);
      break;
    }
    case "error": {
      console.error(event.error);
      break;
    }
  }
}
```
