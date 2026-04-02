# Copilot Usage

A lightweight Windows system tray application that visually shows how much of your monthly GitHub Copilot premium request quota you have consumed.

---

## What it does

- Sits in the **Windows system tray** with the official Copilot icon.
- The icon shows a small **coloured bar** at the bottom indicating quota consumption:
  - 🟢 Green — below 60 %
  - 🟡 Amber — 60–79 %
  - 🔴 Red — 80 % or more
  - ⬜ Grey — data unavailable
- **Single-click** on the tray icon opens a popup with two progress bars:
  - **Premium Requests** — how much of the monthly quota you have used.
  - **Month Progress** — how far through the current month you are.
  
  Comparing the two bars lets you see at a glance whether your usage is ahead of or behind the calendar.
- The popup shows the quota reset date and the time of the last successful refresh.
- Data refreshes automatically every 5 minutes and immediately when the popup is opened.
- **Click the tray icon again** or press the **✕** button to close the popup.
- The popup can be **moved** by dragging it.

---

## Setup

### First run — authorise with GitHub

1. Launch `CopilotUsage.exe`. A Copilot icon appears in the system tray.
2. Right-click the tray icon and choose **Settings**.
3. Click **Authorize with GitHub**.
   - A device code is copied to your clipboard automatically.
   - Your browser opens `github.com/login/device`.
   - Paste the code and authorise **Copilot Usage**.
4. Click **Save**. The app fetches your usage data immediately.

### Subsequent runs

Your access token is stored in:
```
%APPDATA%\CopilotUsage\settings.json
```
The app uses it automatically. No re-authorisation is needed until the token is revoked.

### Re-authorising

1. Right-click the tray → **Settings** → **Re-authorize with GitHub…**  
   Or delete `%APPDATA%\CopilotUsage\settings.json` manually.
2. Follow the first-run steps above.

> **Advanced:** If your organisation's GitHub policy blocks the built-in Copilot Usage OAuth App,
> you can register your own app at `github.com/settings/applications/new` (enable Device Flow) and
> set `"GitHubClientId": "YOUR_CLIENT_ID"` in `%APPDATA%\CopilotUsage\settings.json` manually.

---

## Requirements

- Windows 10 or later
- .NET 10 Desktop Runtime
- An active **GitHub Copilot** subscription (Individual, Business, or Enterprise)

> **Copilot Business / Enterprise:** The premium request quota API requires the **2025-04-01** API version and the Copilot internal session-token endpoint. This is the same API used by the VS Code Copilot Chat extension.

---

## Building from source

```
dotnet build
```

The first build automatically runs `generate-icon.ps1` (requires PowerShell in STA mode, available on any modern Windows) to render the official Copilot icon into `Resources/copilot_icon.ico` so the EXE, title-bar, and tray icons are all identical.

---

## Tray icon context menu

| Item | Action |
|---|---|
| **Settings** | Open the settings / authorisation window |
| **Refresh** | Fetch quota data immediately |
| **About** | Show version information |
| **Exit** | Quit the application |

---

## Demo / test mode

Launch the app with the `--demo` argument to enter demo mode without needing a GitHub token:

```
CopilotUsage.exe --demo
```

In demo mode:
- **Usage** cycles from 0 % → 5 % → 10 % → … → 100 % → 0 %, advancing every 2 seconds (~40 s per full cycle). This lets you observe the tray icon at every usage level.
- **Month date** cycles through interesting calendar snapshots (Jan 1, Feb 14, Feb 28, Mar 1, Mar 15, Jun 30, Dec 31) every 10 seconds. This lets you verify how the month-progress bar and label look at the start, middle, and end of months, including short months like February.
- A yellow **"DEMO MODE"** banner appears at the bottom of the popup window.
- The tray tooltip is prefixed with `[DEMO]`.
- No GitHub API calls are made.
