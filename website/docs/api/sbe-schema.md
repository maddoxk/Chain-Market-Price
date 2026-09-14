---
id: sbe-schema
title: Simple Binary Encoding (SBE) XML Schema
sidebar_label: SBE XML Schema
---

# Simple Binary Encoding (SBE) XML Schema

The exact Simple Binary Encoding schema definition file is located in the repository at [`schemas/market_data.sbe.xml`](https://github.com/maddoxk/Chain-Market-Price/blob/main/schemas/market_data.sbe.xml).

---

## Schema Overview

- **`schemaId`**: `1`
- **`version`**: `1`
- **`byteOrder`**: `littleEndian`

### Message Types
- **Template 101 (`BBOReport`)**: Best Bid and Offer L1 report (92 bytes total: 8 bytes header + 84 bytes payload).
- **Template 104 (`TradeExecutionReport`)**: Trade execution / AMM swap event report (95 bytes total: 8 bytes header + 87 bytes payload).
