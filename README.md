# Copilot Usage

A lightweight Windows system tray application that visually shows how much of your monthly GitHub Copilot premium request quota you have consumed
in comparison to how far through the month you are.

---

## What it does

- Sits in the **Windows system tray** with the official Copilot icon.
- The icon shows a **coloured line tracing its border clockwise** proportional to quota consumption:
  - 🟢 Green — below 60 %
  - 🟡 Amber — 60–79 %
  - 🔴 Red — 80 % or more
  - ⬜ Grey — data unavailable

  ![System tray icon with tooltip](docs/screenshots/Copilot%20Usage%20-%20System%20Tray.png)

- **Single-click** on the tray icon opens a popup with two progress bars:
  - **Premium Requests** — how much of the monthly quota you have used.
  - **Month Progress** — how far through the current month you are.
  
  Comparing the two bars lets you see at a glance whether your usage is ahead of or behind the calendar.

  ![Popup window](docs/screenshots/Copilot%20Usage%20-%20Window.png)

- The popup shows the quota reset date and the time of the last successful refresh.
- Data refreshes automatically every **5 minutes** by default (configurable to 1, 5, 15, or 60 minutes via Settings) and immediately when the popup is opened.
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
   - Paste the code and click **Continue**.
4. GitHub shows an authorisation page for the **Copilot Usage** app. Click **Authorize TobiasW-T**.

   ![GitHub authorisation page](docs/screenshots/Copilot%20Usage%20-%20Authorization%20at%20GitHub.png)

   > **Why does the button say "Authorize TobiasW-T"?**  
   > GitHub labels this button *"Authorize {app-owner}"* for every third-party OAuth App.  
   > `TobiasW-T` is the GitHub account under which the *Copilot Usage* OAuth App is registered — it is **not** authorising that person to access your data.  
   > The app itself (not its owner) receives a token scoped only to the two permissions listed on the page:
   > - **GitHub Copilot → Manage GitHub Copilot** — required to read your premium request quota via the Copilot API.
   > - **Personal user data → Profile information (read-only)** — required to identify your GitHub account.
   >
   > Organisation access shown on the page is **optional** and has no effect on the app's functionality — you can safely ignore it.  
   > The footer note *"Not owned or operated by GitHub"* is standard for all third-party OAuth Apps and simply means GitHub itself did not build this app.

5. Click **Save** in the Settings window. The app fetches your usage data immediately.

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

- Windows 10 or later (x64)
- An active **GitHub Copilot** subscription (Individual, Business, or Enterprise)

> No .NET installation required — the .NET runtime and all WPF native libraries are bundled inside the executable.

> **Copilot Business / Enterprise:** The premium request quota API requires the **2025-04-01** API version and the Copilot internal session-token endpoint. This is the same API used by the VS Code Copilot Chat extension.

---

## Download

Pre-built standalone executables (no .NET required) are available on the [Releases](../../releases) page.
Download `CopilotUsage.exe` from the latest release and run it directly — no installer needed.

---

## Building from source

Requires the [.NET 10 SDK](https://dotnet.microsoft.com/download).

```
dotnet build
```

The first build automatically runs `generate-icon.ps1` (requires PowerShell in STA mode, available on any modern Windows) to render the official Copilot icon into `Resources/copilot_icon.ico` so the EXE, title-bar, and tray icons are all identical.

### Creating a release build

To produce a self-contained, single-file executable (bundles the .NET runtime and all WPF native libraries — no installation required on the target machine):

```powershell
.\build-release.ps1
```

Output: `bin\Publish\win-x64\CopilotUsage.exe` (~180 MB)

Alternatively, use **Visual Studio → Publish** and select the `Release-Win64-SelfContained` profile.

### Publishing a new release on GitHub

1. Run `.\build-release.ps1` and note the output path.
2. Commit and tag the release commit: `git tag v1.0.0 && git push origin v1.0.0`
3. Go to the GitHub repository → **Releases** → **Draft a new release**.
4. Select the tag you just pushed, write release notes, and attach `CopilotUsage.exe`.
5. Publish.

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

---

## Author

Created by [Tobias Wenzel](mailto:tobias_wenzel@trimble.com)  
Homepage: <https://github.com/Trimble-Advanced-Positioning/CopilotUsage>
