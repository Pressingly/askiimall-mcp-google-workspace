# Google Chat MCP — Setup Guide

## 1. Google Cloud setup

In the GCP project that owns the OAuth client (e.g. `askii-sandbox-ase1`):

### Enable APIs

`APIs & Services → Library` → enable:

- **Google Chat API**
- **People API**

People API is required for resolving Chat sender display names; without it sender fields fall back to raw `users/<id>`.

### Configure the Google Chat API

`APIs & Services → Enabled APIs & services → Google Chat API → Configuration` tab.

| Section | Field | Value |
|---|---|---|
| — | Build this Chat app as a Workspace add-on | ✅ checked |
| App status | App status | `LIVE - available to users` |
| Application info | App name | `Askii Chat` |
| Application info | Avatar URL | `https://example.com/chat` (HTTPS, square 1:1 PNG, ≥256×256) |
| Application info | Description | `Askii Chat Integration` (max 40 chars) |
| Interactive features | Enable Interactive features | ✅ on |
| Functionality | Join spaces and group conversations | ✅ checked |
| Connection settings | Deployment | `HTTP endpoint URL` |
| Triggers | Trigger mode | `Use a common HTTP endpoint URL for all triggers` |
| Triggers | HTTP endpoint URL | `https://example.com/chat` (must be HTTPS with a valid cert) |
| Visibility | Make this Chat app available to specific people and groups | unchecked (org-wide) |
| Logs | Log errors to Logging | ✅ checked |


Click **Save** at the bottom of the page.

---

## 2. OAuth scope (Developer Portal — Step 2 Technical Details)

Paste this into the `scope` field (single space-separated string) when registering or updating the service:

```
openid https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/chat.messages.readonly https://www.googleapis.com/auth/chat.messages https://www.googleapis.com/auth/chat.spaces https://www.googleapis.com/auth/chat.memberships.readonly https://www.googleapis.com/auth/contacts.readonly
```

Each scope must also be added to the GCP **OAuth consent screen** (`APIs & Services → OAuth consent screen → Scopes`) for the same project, otherwise Google rejects the consent flow.

---

## 3. Tool filter

Paste this into the `tool_filter` field on the MCPO service record (`mcpo_services.tool_filter`):

```json
{
  "mode": "prefix",
  "prefixes": [
    "list_spaces",
    "get_space",
    "get_messages",
    "get_message",
    "send_message",
    "update_message",
    "delete_message",
    "search_messages",
    "list_members",
    "find_direct_message"
  ]
}
```
