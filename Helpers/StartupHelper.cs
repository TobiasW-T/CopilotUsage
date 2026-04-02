using Microsoft.Win32;

namespace CopilotUsage.Helpers;

internal static class StartupHelper
{
	private const string RegistryKeyPath = @"SOFTWARE\Microsoft\Windows\CurrentVersion\Run";
	private const string AppName = "CopilotUsage";


	public static void ApplyStartupSetting( bool startWithWindows )
	{
		using var key = Registry.CurrentUser.OpenSubKey( RegistryKeyPath, writable: true );
		if ( key == null )
		{
			return;
		}

		if ( startWithWindows )
		{
			var exePath = System.Diagnostics.Process.GetCurrentProcess().MainModule?.FileName;
			if ( exePath != null )
			{
				key.SetValue( AppName, $"\"{exePath}\"" );
			}
		}
		else
		{
			key.DeleteValue( AppName, throwOnMissingValue: false );
		}
	}
}
