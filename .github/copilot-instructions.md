# Copilot Usage — Coding Instructions

These instructions apply to all C# and XAML files in this repository.

---

## Code Style

### Indentation & Spacing
- Use **tabs** for indentation (not spaces), except for XAML files, which should use 4 spaces per indent level.
- Add a **space inside parentheses** for method calls, conditions, and casts:
  ```csharp
  if ( condition )
  DoSomething( arg1, arg2 );
  var x = (int)( value + 1 );
  ```
- Do **not** align assignment operators or values with multiple spaces.
  ```csharp
  // WRONG:
  var foo   = 1;
  var bar   = 2;
  var baz   = 3;

  // CORRECT:
  var foo = 1;
  var bar = 2;
  var baz = 3;
  ```

### Naming Conventions
| Element | Convention | Example |
|---|---|---|
| Instance fields | `m_` prefix + PascalCase | `m_UsagePercent` |
| Mutable static fields | `s_` prefix + PascalCase | `s_CopilotBase` |
| `static readonly` fields (const-like) | PascalCase, no prefix | `JsonOptions`, `MaxRetries` |
| Constants | PascalCase | `IconSize`, `BarHeight` |
| Properties, methods, classes | PascalCase | `GetUsageAsync()` |
| Local variables, parameters | camelCase | `usagePercent` |

### Braces
Every statement body **must** have curly braces, even one-liners:
```csharp
// WRONG:
if ( condition )
    DoSomething();

// CORRECT:
if ( condition )
{
    DoSomething();
}
```
This applies to `if`, `else`, `for`, `foreach`, `while`, `using`, etc.

### Line Endings
- Always use **CRLF** (`\r\n`) line endings.

### Culture & Formatting
- Always pass a **format provider** when parsing or formatting dates/times/numbers to suppress CA warnings and ensure culture-invariant behaviour:
  ```csharp
  DateTime.Now.ToString( "HH:mm:ss", CultureInfo.InvariantCulture )
  int.Parse( s, CultureInfo.InvariantCulture )
  ```
- Month names visible to the user must always be in **English** (`CultureInfo.InvariantCulture`).

### Classes
- Every class must be in its **own file**.
- Nested classes are not allowed; extract them to top-level.
- Visibility of extracted internal types should be `internal`.

---

## Architecture

- **`SettingsService`** is `static` — it is entirely stateless (reads/writes a JSON settings file). Keep it static; no DI needed.
- **`TrayApplicationContext`** owns the `NotifyIcon` and the WPF `Application` lifetime.
- **`UsageViewModel`** is the single data source for the popup window, implementing `INotifyPropertyChanged`.
- **`TrayIconHelper`** renders the WPF `CopilotIcon` DrawingImage resource to a GDI+ bitmap for the tray and provides `GetWpfImageSource()` for window icons.
- **`GitHubCopilotService`** handles the two-step API flow: GitHub OAuth token → Copilot session token → quota.

---

## WPF / XAML
- Use `StaticResource` for converters and styles defined in `App.xaml`.
- In `DockPanel`, children that dock to non-default sides (`Right`, `Top`, `Bottom`) **must be declared before** the fill child, or docking will not work.
- The `CopilotIcon` `DrawingImage` in `App.xaml` is the single source of truth for the Copilot icon vector. Use `{StaticResource CopilotIcon}` in XAML and `TrayIconHelper.GetWpfImageSource()` in code.
