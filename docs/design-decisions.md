# Design Decisions & Technical Findings

This document records key decisions made during development and technical findings that were non-obvious to discover. Its purpose is to help future contributors (human or AI) avoid re-evaluating things that have already been solved.

---

## GitHub Copilot API

### Correct endpoint

The quota is **not** available via the public GitHub REST API. The correct endpoint is an internal Copilot one:

```
GET https://api.github.com/copilot_internal/user
Authorization: token {github_oauth_token}
X-GitHub-Api-Version: 2025-04-01
```

**Why not the public API?**  
The public `GET /user/copilot_billing` endpoint exists but does not expose the premium request quota (remaining/used counts). It returns only plan metadata.

**API version matters.**  
Using the older `2022-11-28` version returns 404 for Copilot Business/Enterprise accounts. The `2025-04-01` version is required for quota data to be returned.

### Session token exchange

The above endpoint returns an OAuth token, not a Copilot session token. The actual quota is read from:

```
GET https://api.github.com/copilot_internal/user
```

...using the **GitHub OAuth token directly** (not a Copilot session token). Previous attempts to exchange for a session token via `https://api.github.com/copilot_internal/v2/token` resulted in 404 for organisation-managed accounts.

The current implementation uses the GitHub OAuth token directly with the `copilot_internal/user` endpoint and API version `2025-04-01`. This matches what the VS Code Copilot Chat extension does.

### Copilot Business / Enterprise

Organisation-managed accounts **do** expose per-user quota via this endpoint. The key is:
1. Using the correct API version (`2025-04-01`)
2. Using the OAuth token directly (no session token exchange)
3. Ensuring the OAuth token has the `copilot` scope

If a 404 is returned after re-authorising, the token was likely issued before the `copilot` scope was added — revoke and re-authorise.

---

## Authentication — OAuth Device Flow

GitHub's Device Flow is used instead of a Personal Access Token (PAT) for a better user experience:

1. App calls `https://github.com/login/device/code` with `scope=copilot read:user`
2. User gets a device code (copied to clipboard automatically)
3. Browser opens `https://github.com/login/device`
4. User pastes the code and approves
5. App polls `https://github.com/login/oauth/access_token` until approved

**Client ID used:** `Iv1.b507a08c87ecfe98` (GitHub CLI public client ID, widely used for personal tooling)

If an organisation blocks third-party OAuth apps, the user can provide their own OAuth App client ID in Settings.

**Scope required:** `copilot` — this scope was added by GitHub relatively recently. Older tokens without this scope will get 404 from the quota endpoint.

---

## Icon Architecture

### Problem
Three icon surfaces need to look identical:
1. EXE file icon (embedded at compile time as `.ico`)
2. Window title-bar icon (runtime)
3. System tray icon (runtime, GDI+)

### Solution
- The single source of truth is the **`CopilotIcon` DrawingImage** defined in `App.xaml`, containing the official Copilot octicon SVG paths from [primer/octicons](https://github.com/primer/octicons).
- **Tray icon:** `TrayIconHelper.GetCopilotBase()` renders the DrawingImage via `DrawingVisual` + `RenderTargetBitmap` → GDI+ `Bitmap`. Result is cached as `s_CopilotBase`.
- **Window title-bar icon:** `TrayIconHelper.GetWpfImageSource()` converts `s_CopilotBase` to a WPF `BitmapSource` via `CreateBitmapSourceFromHBitmap`. Set on `Window.Icon`.
- **EXE file icon:** `generate-icon.ps1` runs as a pre-build step (PowerShell STA, loads WPF assemblies) and renders the same DrawingImage to 16×16 and 32×32 PNGs, then assembles a PNG-in-ICO file (`Resources/copilot_icon.ico`). Because it uses the same WPF rendering pipeline, the result is pixel-identical to the runtime icon.

### Why PNG-in-ICO?
ICO files can embed either BMP or PNG image data. PNG-in-ICO is supported since Windows Vista, produces smaller files, and supports full alpha channel without the BMP premultiplied-alpha complexity. For 16×16 and 32×32, the resulting file is ~2 KB vs ~6 KB for BMP.

### Tray icon — liquid level fill
The tray icon uses a three-layer composition to visualise quota as a "liquid level" fill:
1. **Dark navy rounded-rect background** — drawn in GDI+ with `GraphicsPath.AddArc` to match the icon's corner radius.
2. **Coloured fill from the bottom up** — a rectangle grown upward proportional to `usagePercent`, clipped to the rounded-rect path. Colours: green < 60 %, amber 60–80 %, red ≥ 80 %, grey when data is unavailable. Fill alpha = 180 so the Copilot paths on top remain readable at all levels.
3. **White Copilot paths** — rendered from a second cached bitmap `s_CopilotPaths` which renders only child `[1]` of the `DrawingGroup` (the white geometry), skipping the background `[0]`. This bitmap has a transparent background and is composited on top last.

`GetCopilotBase()` (full icon including background) is kept for `GetWpfImageSource()` which provides the About/title-bar icon.

---

## WPF + Windows Forms Mixed Mode

The app uses **both** `UseWPF` and `UseWindowsForms` in the csproj:
- `UseWindowsForms` is required for `System.Windows.Forms.NotifyIcon` (system tray).
- `UseWPF` is required for the popup and settings windows.

Both SDKs add global implicit usings, which causes `System.Drawing.Color` and `System.Windows.Media.Color` to be in scope simultaneously. In `TrayIconHelper.cs`, this is resolved with a type alias:
```csharp
using DrawingColor = System.Drawing.Color;
```
All WPF types in that file are fully-qualified to avoid further ambiguity.

---

## Static SettingsService

`SettingsService` is `static` because:
- It has no instance state (reads/writes a fixed JSON path in `%APPDATA%`).
- There is no need for multiple instances or mocking in tests.
- Making it static simplifies all call sites (no DI or constructor injection needed).

For a more complex or testable application, consider making it non-static and injecting it via an interface.

---

## Popup Window Behaviour

The popup is a `WindowStyle="None"` borderless WPF window positioned near the tray icon:
- **Opens** on single-click of the tray icon.
- **Closes** on second click of the tray icon, or when the ✕ button is clicked.
- It does **not** close when losing focus — this allows the user to keep it open while using other apps.
- It can be dragged (`Border.MouseLeftButtonDown` → `DragMove()`).
- `ShowInTaskbar="False"` to avoid a taskbar button.

Positioning is calculated from `Screen.PrimaryScreen.WorkingArea` and `SystemInformation.PrimaryMonitorMaximizedWindowSize` to place the window near the bottom-right corner of the primary monitor, above the taskbar.

---

## DockPanel Child Order

In WPF `DockPanel`, the **last child** fills remaining space regardless of its `DockPanel.Dock` value. Children that must dock to a specific edge must be declared **before** the fill child. For the popup footer:
```xml
<DockPanel>
    <StackPanel DockPanel.Dock="Right" .../>  <!-- FIRST: docks right -->
    <TextBlock .../>                           <!-- LAST: fills remaining left space -->
</DockPanel>
```

Declaring them in the opposite order causes the `DockPanel.Dock="Right"` to be ignored.
